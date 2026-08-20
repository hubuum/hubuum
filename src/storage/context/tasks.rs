use super::*;

#[async_trait]
impl TaskQueueStorage for StorageHandle {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "create", async {
            dispatch_backend!(self, |backend| backend.create_task(request).await)
        })
        .await
    }

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "get_access", async {
            dispatch_backend!(self, |backend| { backend.get_task_access(task_id).await })
        })
        .await
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StorageTaskPage, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "list", async {
            dispatch_backend!(self, |backend| backend.list_tasks(query).await)
        })
        .await
    }

    async fn list_task_events(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageTaskEventPage, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "list_events", async {
            dispatch_backend!(self, |backend| backend.list_task_events(query).await)
        })
        .await
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageImportTaskResultPage, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "list_import_results", async {
            dispatch_backend!(self, |backend| {
                backend.list_import_task_results(query).await
            })
        })
        .await
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "list_export_outputs", async {
            dispatch_backend!(self, |backend| {
                backend.list_export_output_summaries(task_ids).await
            })
        })
        .await
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "list_backup_outputs", async {
            dispatch_backend!(self, |backend| {
                backend.list_backup_output_summaries(task_ids).await
            })
        })
        .await
    }

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "get_export_summary", async {
            dispatch_backend!(self, |backend| {
                backend.get_export_output_summary(task_id).await
            })
        })
        .await
    }

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "get_backup_summary", async {
            dispatch_backend!(self, |backend| {
                backend.get_backup_output_summary(task_id).await
            })
        })
        .await
    }

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "get_export_output", async {
            dispatch_backend!(self, |backend| { backend.get_export_output(task_id).await })
        })
        .await
    }

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        self.observe_storage_call(self.backend_name(), "tasks", "get_backup_output", async {
            dispatch_backend!(self, |backend| { backend.get_backup_output(task_id).await })
        })
        .await
    }
}

#[async_trait]
impl TaskExecutionStorage for StorageHandle {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        self.observe_storage_call(self.backend_name(), "task_execution", "claim", async {
            dispatch_backend!(self, |backend| {
                backend.claim_next_task(lease_duration).await
            })
        })
        .await
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "task_execution",
            "renew_lease",
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
            "task_execution",
            "recover_leases",
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
            "task_execution",
            "append_event",
            async { dispatch_backend!(self, |backend| { backend.append_task_event(event).await }) },
        )
        .await
    }

    async fn update_task_state(
        &self,
        update: StorageTaskStateUpdate,
    ) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "task_execution",
            "update_state",
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
        self.observe_storage_call(self.backend_name(), "task_execution", "complete", async {
            dispatch_backend!(self, |backend| { backend.complete_task(completion).await })
        })
        .await
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        self.observe_storage_call(self.backend_name(), "task_execution", "fail", async {
            dispatch_backend!(self, |backend| backend.fail_task(failure).await)
        })
        .await
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "task_execution",
            "purge_export_outputs",
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
            "task_execution",
            "purge_backup_outputs",
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
    async fn snapshot_backup(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        self.observe_storage_call(self.backend_name(), "backup_snapshots", "snapshot", async {
            dispatch_backend!(self, |backend| {
                backend.snapshot_backup(include_history).await
            })
        })
        .await
    }
}
