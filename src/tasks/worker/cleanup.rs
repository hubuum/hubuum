//! One bounded maintenance loop, independent of foreground task claims.

use super::*;

pub(super) fn spawn(context: AppContext, poll_interval: Duration) {
    spawn_background_worker("task-retention-maintenance", move |shutdown| {
        let system = actix_rt::System::new();
        system.block_on(async move {
            run(task_worker_context(context), poll_interval, shutdown).await;
        });
    });
}

pub(super) async fn run(context: AppContext, poll_interval: Duration, shutdown: ShutdownSignal) {
    let tasks = Mutex::new(CleanupSchedule::default());
    let artifacts = Mutex::new(CleanupSchedule::default());
    loop {
        if shutdown.is_requested() {
            break;
        }
        let work = Box::pin(iteration(&context, &tasks, &artifacts));
        let retry_soon =
            match with_storage_call_site(&context, StorageCallSite::TaskWorker, work).await {
                Ok(retry_soon) => retry_soon,
                Err(error) => {
                    error!(message = "Task maintenance iteration failed", error = %error);
                    false
                }
            };
        // Release the cross-process publication lock between bounded passes,
        // including productive ones. A runtime yield alone cannot give another
        // process an opportunity to publish a hold, admission, or checkpoint.
        let pause = if retry_soon {
            poll_interval.min(Duration::from_millis(100))
        } else {
            poll_interval
        };
        tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            _ = sleep(pause) => {}
        }
    }
}

async fn iteration(
    context: &AppContext,
    tasks: &Mutex<CleanupSchedule>,
    artifacts: &Mutex<CleanupSchedule>,
) -> Result<bool, ApiError> {
    let interval = task_worker_settings().export_output_cleanup_interval();
    // Protected receipt/detail deadlines can be much shorter than the legacy
    // output interval. Check their current authority each worker tick, including
    // after an idle pass or activation while the runtime is already running.
    // Unconfigured runtimes still avoid backend calls while output cleanup is idle.
    if !context.task_receipts().is_configured()
        && !due(tasks, interval)?
        && !due(artifacts, interval)?
    {
        return Ok(false);
    }
    let storage = context.backend();
    let _activity = MaintenanceActivityGuard::begin();
    let (permit, maintenance) = storage
        .admit_application_work_with_maintenance()
        .await?
        .into_parts();
    if !maintenance.is_normal() {
        return Ok(false);
    }
    let scope = storage
        .current_execution_scope()
        .with_application_access(permit);
    let work = Box::pin(maybe_cleanup_expired_task_outputs(
        context, tasks, artifacts,
    ));
    storage
        .run_in_scope(scope, track_application_completion(work))
        .await
}

fn due(state: &Mutex<CleanupSchedule>, interval: Duration) -> Result<bool, ApiError> {
    state
        .lock()
        .map(|schedule| schedule.is_due(interval))
        .map_err(|_| ApiError::InternalServerError("Cleanup state lock poisoned".to_owned()))
}

#[cfg(test)]
mod tests;
