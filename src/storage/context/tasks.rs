use super::*;
use hubuum_storage_core::StorageTaskClaimRequest;
use hubuum_storage_core::{
    ProtectedTaskAdmission, StorageBackupReplay, StorageSchemaProfileArtifact,
    StorageTaskCancellationOutcome, StorageTaskCancellationRequest, StorageTaskCapacitySnapshot,
    StorageTaskExecutionAdmission, StorageTaskExecutionObservation, StorageTaskPolicy,
    StorageTaskPolicyUpdate, StorageTaskRemoteDispatch, StorageTaskSubmissionOutcome,
    TaskActivationOutcome, TaskAdmissionPolicy, TaskCapacityStorage, TaskSubmissionLookup,
    TaskSubmissionResolution,
    StorageTaskResumeOutcome, StorageTaskResumeRequest, StorageTaskWait, StorageTaskWaitRequest,
};

#[async_trait]
impl TaskCapacityStorage for StorageHandle {
    async fn initialize_task_policy(
        &self,
        policy: TaskAdmissionPolicy,
    ) -> Result<StorageTaskPolicy, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskCapacity,
            "initialize_task_policy",
            async {
                dispatch_backend!(self, |backend| backend.initialize_task_policy(policy).await)
            },
        )
        .await
    }

    async fn task_policy(&self) -> Result<StorageTaskPolicy, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskCapacity,
            "task_policy",
            async { dispatch_backend!(self, |backend| backend.task_policy().await) },
        )
        .await
    }

    async fn task_capacity_snapshot(&self) -> Result<StorageTaskCapacitySnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskCapacity,
            "task_capacity_snapshot",
            async { dispatch_backend!(self, |backend| backend.task_capacity_snapshot().await) },
        )
        .await
    }

    async fn replace_task_policy(
        &self,
        update: StorageTaskPolicyUpdate,
    ) -> Result<StorageTaskPolicy, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskCapacity,
            "replace_task_policy",
            async { dispatch_backend!(self, |backend| backend.replace_task_policy(update).await) },
        )
        .await
    }
}

#[async_trait]
impl TaskQueueStorage for StorageHandle {
    async fn lookup_task_submission(
        &self,
        lookup: TaskSubmissionLookup,
    ) -> Result<TaskSubmissionResolution, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "lookup_task_submission",
            async {
                dispatch_backend!(self, |backend| backend.lookup_task_submission(lookup).await)
            },
        )
        .await
    }

    async fn activate_task_submission(
        &self,
        request: ProtectedTaskAdmission,
    ) -> Result<TaskActivationOutcome, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "activate_task_submission",
            async {
                dispatch_backend!(self, |backend| backend
                    .activate_task_submission(request)
                    .await)
            },
        )
        .await
    }

    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTaskSubmissionOutcome, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "create_task",
            async { dispatch_backend!(self, |backend| backend.create_task(request).await) },
        )
        .await
    }

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "get_task_access",
            async { dispatch_backend!(self, |backend| { backend.get_task_access(task_id).await }) },
        )
        .await
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StoragePage<StorageTask>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "list_tasks",
            async { dispatch_backend!(self, |backend| backend.list_tasks(query).await) },
        )
        .await
    }

    async fn list_task_events(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageTaskEvent>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "list_task_events",
            async { dispatch_backend!(self, |backend| backend.list_task_events(query).await) },
        )
        .await
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageImportTaskResult>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "list_import_task_results",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_import_task_results(query).await
                })
            },
        )
        .await
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "list_export_output_summaries",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_export_output_summaries(task_ids).await
                })
            },
        )
        .await
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "list_backup_output_summaries",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_backup_output_summaries(task_ids).await
                })
            },
        )
        .await
    }

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "get_export_output_summary",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_export_output_summary(task_id).await
                })
            },
        )
        .await
    }

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "get_backup_output_summary",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_backup_output_summary(task_id).await
                })
            },
        )
        .await
    }

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "get_export_output",
            async {
                dispatch_backend!(self, |backend| { backend.get_export_output(task_id).await })
            },
        )
        .await
    }

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "get_backup_output",
            async {
                dispatch_backend!(self, |backend| { backend.get_backup_output(task_id).await })
            },
        )
        .await
    }
}

#[async_trait]
impl TaskExecutionStorage for StorageHandle {
    async fn wait_task(
        &self,
        request: StorageTaskWaitRequest,
    ) -> Result<StorageTaskWait, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "wait_task",
            async { dispatch_backend!(self, |backend| backend.wait_task(request).await) },
        )
        .await
    }

    async fn resume_task(
        &self,
        request: StorageTaskResumeRequest,
    ) -> Result<StorageTaskResumeOutcome, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "resume_task",
            async { dispatch_backend!(self, |backend| backend.resume_task(request).await) },
        )
        .await
    }

    async fn get_schema_profile_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageSchemaProfileArtifact>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "get_schema_profile_output",
            async {
                dispatch_backend!(self, |backend| backend
                    .get_schema_profile_output(task_id)
                    .await)
            },
        )
        .await
    }
    async fn purge_expired_schema_profile_outputs(&self) -> Result<usize, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "purge_expired_schema_profile_outputs",
            async {
                dispatch_backend!(self, |backend| backend
                    .purge_expired_schema_profile_outputs()
                    .await)
            },
        )
        .await
    }

    async fn request_task_cancellation(
        &self,
        request: StorageTaskCancellationRequest,
    ) -> Result<StorageTaskCancellationOutcome, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "request_task_cancellation",
            async {
                dispatch_backend!(self, |backend| backend
                    .request_task_cancellation(request)
                    .await)
            },
        )
        .await
    }

    async fn admit_task_execution(
        &self,
        request: StorageTaskExecutionAdmission,
    ) -> Result<StorageTaskExecutionObservation, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "admit_task_execution",
            async {
                dispatch_backend!(self, |backend| backend.admit_task_execution(request).await)
            },
        )
        .await
    }

    async fn poll_task_execution(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTaskExecutionObservation, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "poll_task_execution",
            async { dispatch_backend!(self, |backend| backend.poll_task_execution(lease).await) },
        )
        .await
    }

    async fn acknowledge_task_stop(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "acknowledge_task_stop",
            async { dispatch_backend!(self, |backend| backend.acknowledge_task_stop(lease).await) },
        )
        .await
    }

    async fn begin_remote_dispatch(
        &self,
        request: StorageTaskRemoteDispatch,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "begin_remote_dispatch",
            async {
                dispatch_backend!(self, |backend| backend.begin_remote_dispatch(request).await)
            },
        )
        .await
    }

    async fn claim_next_task(
        &self,
        request: StorageTaskClaimRequest,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "claim_next_task",
            async { dispatch_backend!(self, |backend| { backend.claim_next_task(request).await }) },
        )
        .await
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "renew_task_lease",
            async {
                dispatch_backend!(self, |backend| {
                    backend.renew_task_lease(lease, lease_duration).await
                })
            },
        )
        .await
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "recover_expired_task_leases",
            async {
                dispatch_backend!(self, |backend| {
                    backend.recover_expired_task_leases(batch_size).await
                })
            },
        )
        .await
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "append_task_event",
            async { dispatch_backend!(self, |backend| { backend.append_task_event(event).await }) },
        )
        .await
    }

    async fn update_task_state(
        &self,
        update: StorageTaskActiveUpdate,
    ) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "update_task_state",
            async {
                dispatch_backend!(self, |backend| { backend.update_task_state(update).await })
            },
        )
        .await
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "complete_task",
            async {
                dispatch_backend!(self, |backend| { backend.complete_task(completion).await })
            },
        )
        .await
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "fail_task",
            async { dispatch_backend!(self, |backend| backend.fail_task(failure).await) },
        )
        .await
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "purge_expired_export_outputs",
            async {
                dispatch_backend!(self, |backend| {
                    backend.purge_expired_export_outputs().await
                })
            },
        )
        .await
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskExecution,
            "purge_expired_backup_outputs",
            async {
                dispatch_backend!(self, |backend| {
                    backend.purge_expired_backup_outputs().await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl BackupSnapshotStorage for StorageHandle {
    async fn capture_backup_replay(
        &self,
        include_history: bool,
        budget: StorageBackupBudget,
    ) -> Result<StorageBackupReplay, StorageError> {
        debug!(
            backend = self.backend_name(),
            max_output_bytes = budget.max_bytes(),
            max_capture_rows = budget.max_rows(),
            include_history,
            "Capturing backup with configured resource limits"
        );
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::BackupSnapshot,
            "capture_backup_replay",
            async {
                dispatch_backend!(self, |backend| {
                    backend.capture_backup_replay(include_history, budget).await
                })
            },
        )
        .await
    }
}
