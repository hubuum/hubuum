mod execution;
mod helpers;
mod planning;
mod preload;
mod remote_call;
mod resolution;
mod settings;
mod types;
mod worker;

pub use helpers::{idempotency_key_from_headers, request_hash};
#[cfg(feature = "integration-test-support")]
pub(crate) use remote_call::{enter_local_remote_target_test, exit_local_remote_target_test};
pub(crate) use settings::TaskLeaseDuration;
pub use settings::{TaskWorkerSettings, TaskWorkerSettingsBuilder};
pub use worker::{
    ensure_task_worker_running, ensure_task_worker_running_with_settings,
    initialize_task_worker_settings, kick_task_worker,
};

#[cfg(test)]
mod tests;
