use hubuum_storage_core::{
    StorageTaskKind, StorageValidationError, TaskAdmissionPolicy, TaskCapacityLimit,
    TaskKindCapacity,
};

use super::AppConfig;

impl AppConfig {
    /// Legacy settings seed the shared policy once. A later process startup
    /// cannot replace the deployment's committed policy revision.
    pub fn initial_task_admission_policy(
        &self,
    ) -> Result<TaskAdmissionPolicy, StorageValidationError> {
        let mut policy = TaskAdmissionPolicy::default();
        for (kind, value) in [
            (
                StorageTaskKind::Import,
                self.import_max_active_tasks_per_user,
            ),
            (
                StorageTaskKind::Export,
                self.export_max_active_tasks_per_user,
            ),
            (
                StorageTaskKind::Backup,
                self.backup_max_active_tasks_per_user,
            ),
            (
                StorageTaskKind::RemoteCall,
                self.remote_call_max_active_tasks_per_user,
            ),
        ] {
            let capacity = policy.for_kind(kind).capacity();
            policy = policy.with_kind(TaskKindCapacity::new(
                kind,
                capacity,
                TaskCapacityLimit::new(value as u64)?,
            ));
        }
        Ok(policy)
    }
}
