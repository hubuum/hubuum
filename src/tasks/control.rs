//! Cooperative application execution with a separately scheduled durable-control monitor.

use std::future::Future;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::task::JoinHandle;

use hubuum_storage_core::{
    StorageExecutionScope, StorageTaskExecutionAdmission, TaskExecutionStorage,
};
use hubuum_task_core::{TaskExecutionContext, TaskExecutionLimit, TaskStopHandle, TaskStopReason};

use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::services::tasks::ClaimedTask;
use crate::storage::{storage_handle, with_storage_execution_scope};

tokio::task_local! {
    static EXECUTION: TaskExecutionContext;
}

pub(crate) fn execution_context() -> Option<TaskExecutionContext> {
    EXECUTION.try_with(Clone::clone).ok()
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

pub(crate) async fn wait_for_stop(execution: &TaskExecutionContext) -> TaskStopReason {
    loop {
        if let Err(reason) = execution.check() {
            return reason;
        }
        tokio::time::sleep(execution.remaining().min(Duration::from_millis(50))).await;
    }
}

/// Lease renewals and stop requests must keep running while the task runtime is busy.
static TASK_LEASE_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("task-lease-heartbeat")
        .enable_all()
        .build()
        .expect("task lease heartbeat runtime must start")
});

pub(super) fn spawn_lease_monitor(
    monitor: impl Future<Output = ()> + Send + 'static,
) -> JoinHandle<()> {
    TASK_LEASE_RUNTIME.spawn(monitor)
}

struct ControlMonitor {
    poller: JoinHandle<()>,
    stop: TaskStopHandle,
}
impl Drop for ControlMonitor {
    fn drop(&mut self) {
        // Losing the lease or shutting down drops the executor and its monitor.
        // Signal any admitted work that still holds a clone of this task's context.
        self.stop.request_stop(TaskStopReason::Cancelled);
        self.poller.abort();
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
    let monitor_stop = stop.clone();
    let poller = spawn_lease_monitor(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if let Ok(control) = backend.poll_task_execution(lease.clone()).await
                && let Some(reason) = control.stop_reason()
            {
                monitor_stop.request_stop(reason);
                break;
            }
        }
    });
    let _monitor = ControlMonitor { poller, stop };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::auth_interactions::oidc::renew;
    use crate::tests::TestContext;
    use diesel::sql_types::{Bool, Integer};
    use diesel_async::RunQueryDsl;
    use hubuum_domain::PrincipalId;
    use hubuum_storage_core::{
        RuntimeSettingsStorage, StorageAuthenticationProvider, StorageAuthenticationProviderKind,
        StorageRuntimeSettingsGroup,
    };
    use hubuum_storage_postgres::with_transaction;
    use serde_json::json;

    #[tokio::test]
    async fn dropping_the_executor_stops_its_detached_admissions() {
        let (task, stop) = TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
        let monitor = ControlMonitor {
            poller: tokio::spawn(std::future::pending()),
            stop,
        };
        drop(monitor);
        assert_eq!(task.check(), Err(TaskStopReason::Cancelled));
    }

    #[tokio::test]
    async fn cancelling_a_task_interrupts_its_blocked_oidc_claim() {
        let context = TestContext::new().await;
        let principal = PrincipalId::new(context.normal_user.id).unwrap();
        let storage = storage_handle(&context.pool);
        let revision = storage
            .load_runtime_settings()
            .await
            .unwrap()
            .record(StorageRuntimeSettingsGroup::Authentication)
            .revision();
        let definition = json!({
            "issuer": "https://id.example",
            "client_id": "hubuum",
            "client_authentication": "client_secret_basic",
            "scopes": ["openid"],
            "signing_algorithms": ["RS256"],
            "retain_refresh_tokens": true,
            "admission": {"mode": "existing_only"},
            "membership": {
                "mode": "local_only",
                "refresh_interval_seconds": 300,
                "max_stale_seconds": 900
            }
        });
        let provider = StorageAuthenticationProvider::try_new(
            "oidc",
            StorageAuthenticationProviderKind::Oidc,
            definition,
        )
        .unwrap();
        let (task, stop) = TaskExecutionContext::new(Duration::from_secs(60)).unwrap();
        with_transaction(&context.pool, async move |connection| {
            diesel::sql_query("SELECT id FROM principals WHERE id=$1 FOR UPDATE")
                .bind::<Integer, _>(principal.id())
                .execute(connection)
                .await?;
            let cancel = async {
                loop {
                    let blocked = diesel::dsl::sql::<Bool>(
                        "EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted
                         AND pg_backend_pid() = ANY(pg_blocking_pids(pid)))",
                    );
                    if diesel::select(blocked)
                        .get_result::<bool>(connection)
                        .await
                        .unwrap()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                stop.request_stop(TaskStopReason::Cancelled);
            };
            let renewal = EXECUTION.scope(task, renew(&storage, principal, revision, &provider));
            let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::join!(renewal, cancel)
            })
            .await
            .expect("renewal must return while the principal row is still locked");
            assert!(matches!(
                result,
                Err(ApiError::TaskStopped(TaskStopReason::Cancelled))
            ));
            Ok::<_, diesel::result::Error>(())
        })
        .await
        .unwrap();
    }
}
