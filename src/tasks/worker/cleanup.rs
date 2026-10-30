//! One bounded maintenance loop, independent of foreground task claims.

use super::*;

pub(super) fn spawn(context: AppContext, poll_interval: Duration) {
    spawn_background_worker("task-retention-maintenance", move |shutdown| {
        let system = actix_rt::System::new();
        system.block_on(async move {
            let context = task_worker_context(context);
            let tasks = Mutex::new(CleanupSchedule::default());
            let artifacts = Mutex::new(CleanupSchedule::default());
            loop {
                if shutdown.is_requested() {
                    break;
                }
                let work = Box::pin(iteration(&context, &tasks, &artifacts));
                let result =
                    with_storage_call_site(&context, StorageCallSite::TaskWorker, work).await;
                if shutdown.is_requested() {
                    break;
                }
                match result {
                    Ok(true) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        error!(message = "Task maintenance iteration failed", error = %error)
                    }
                }
                tokio::select! {
                    biased;
                    _ = shutdown.requested() => break,
                    _ = sleep(poll_interval) => {}
                }
            }
        });
    });
}

async fn iteration(
    context: &AppContext,
    tasks: &Mutex<CleanupSchedule>,
    artifacts: &Mutex<CleanupSchedule>,
) -> Result<bool, ApiError> {
    let interval = task_worker_settings().export_output_cleanup_interval();
    // Idle schedule checks require no backend call. Reservation and application
    // admission are still checked before any actual maintenance operation.
    if !due(tasks, interval)? && !due(artifacts, interval)? {
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
