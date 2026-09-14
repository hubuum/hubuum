//! Cooperative application execution with a separately scheduled durable-control monitor.

use std::future::Future;
use std::time::Duration;

use hubuum_storage_core::{
    StorageExecutionScope, StorageTaskExecutionAdmission, TaskExecutionStorage,
};
use hubuum_task_core::{TaskExecutionContext, TaskExecutionLimit, TaskStopReason};

use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::services::tasks::ClaimedTask;
use crate::storage::{storage_handle, with_storage_execution_scope};

tokio::task_local! {
    static EXECUTION: TaskExecutionContext;
}

pub(crate) fn checkpoint() -> Result<(), ApiError> {
    EXECUTION
        .try_with(TaskExecutionContext::check)
        .unwrap_or(Ok(()))
        .map_err(ApiError::from)
}

/// Use only around HTTP requests, which may have an explicitly recorded remote
/// effect. Database operations drain in their adapter and bounded render jobs
/// are awaited until their subprocess has been reaped.
pub(crate) async fn cancellable<F, T>(future: F) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, ApiError>>,
{
    let execution = EXECUTION.try_with(Clone::clone).ok();
    let Some(execution) = execution else {
        return future.await;
    };
    execution.check()?;
    tokio::select! {
        result = future => result,
        reason = wait_for_stop(&execution) => Err(ApiError::TaskStopped(reason)),
    }
}

async fn wait_for_stop(execution: &TaskExecutionContext) -> TaskStopReason {
    loop {
        if let Err(reason) = execution.check() {
            return reason;
        }
        tokio::time::sleep(execution.remaining().min(Duration::from_millis(50))).await;
    }
}

struct ControlMonitor(tokio::task::JoinHandle<()>);
impl Drop for ControlMonitor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn execute<F, T>(
    context: &AppContext,
    task: &ClaimedTask,
    limit: TaskExecutionLimit,
    future: F,
) -> Result<T, ApiError>
where
    F: Future<Output = Result<T, ApiError>>,
{
    let control = storage_handle(context)
        .admit_task_execution(StorageTaskExecutionAdmission::new(
            task.lease().clone(),
            limit,
        ))
        .await?;
    let remaining = control.remaining().ok_or_else(|| {
        ApiError::InternalServerError("Admitted execution has no deadline".into())
    })?;
    let (execution, stop) = TaskExecutionContext::new(remaining)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    if let Some(reason) = control.stop_reason() {
        stop.request_stop(reason);
    }
    let backend = context.backend().clone();
    let lease = task.lease().clone();
    let _monitor = ControlMonitor(super::worker::task_lease_runtime().spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if let Ok(control) = backend.poll_task_execution(lease.clone()).await
                && let Some(reason) = control.stop_reason()
            {
                stop.request_stop(reason);
                break;
            }
        }
    }));
    let scope = StorageExecutionScope::default().with_task_execution(Some(execution.clone()));
    EXECUTION
        .scope(
            execution,
            with_storage_execution_scope(context, scope, async {
                checkpoint()?;
                future.await
            }),
        )
        .await
}
