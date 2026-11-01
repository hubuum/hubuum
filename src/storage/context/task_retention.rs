use super::*;
use hubuum_storage_core::{
    ControlCheckpointCapture, ProtectedReceiptExpiry, ProtectedTaskPurge,
    ProtectedTaskReapplication, ReceiptExpiryBatch, ReceiptExpirySelection, TaskAdmissionTicket,
    TaskControlChange, TaskControlRecovery, TaskControlRevision, TaskControlState, TaskHoldChange,
    TaskLegacyReceiptBinding, TaskLegacyReceiptCandidate, TaskPurgeBudget, TaskPurgeProgress,
    TaskPurgeSelection, TaskPurgeTicket, TaskReapplicationProgress, TaskReceiptActivation,
    TaskRetentionPolicy, TaskRetentionPolicyChange, TaskRetentionStorage,
};

#[async_trait]
impl TaskRetentionStorage for StorageHandle {
    async fn prepare_expired_task_outputs(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "prepare_expired_task_outputs",
            async {
                dispatch_backend!(self, |backend| backend
                    .prepare_expired_task_outputs(selection)
                    .await)
            },
        )
        .await
    }

    async fn read_task_receipt_activation(&self) -> Result<TaskReceiptActivation, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "read_task_receipt_activation",
            async {
                dispatch_backend!(self, |backend| backend.read_task_receipt_activation().await)
            },
        )
        .await
    }

    async fn list_pending_task_admissions(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskAdmissionTicket>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "list_pending_task_admissions",
            async {
                dispatch_backend!(self, |backend| backend
                    .list_pending_task_admissions(limit)
                    .await)
            },
        )
        .await
    }

    async fn read_task_control(&self) -> Result<TaskControlState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "read_task_control",
            async { dispatch_backend!(self, |backend| backend.read_task_control().await) },
        )
        .await
    }

    async fn capture_control_checkpoint(&self) -> Result<ControlCheckpointCapture, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "capture_control_checkpoint",
            async { dispatch_backend!(self, |backend| backend.capture_control_checkpoint().await) },
        )
        .await
    }

    async fn commit_task_control(
        &self,
        change: TaskControlChange,
    ) -> Result<TaskControlState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "commit_task_control",
            async { dispatch_backend!(self, |backend| backend.commit_task_control(change).await) },
        )
        .await
    }

    async fn recover_task_control(
        &self,
        change: TaskControlRecovery,
    ) -> Result<TaskControlRevision, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "recover_task_control",
            async { dispatch_backend!(self, |backend| backend.recover_task_control(change).await) },
        )
        .await
    }

    async fn read_task_retention_policy(&self) -> Result<TaskRetentionPolicy, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "read_task_retention_policy",
            async { dispatch_backend!(self, |backend| backend.read_task_retention_policy().await) },
        )
        .await
    }

    async fn replace_task_retention_policy(
        &self,
        change: TaskRetentionPolicyChange,
    ) -> Result<TaskControlRevision, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "replace_task_retention_policy",
            async {
                dispatch_backend!(self, |backend| backend
                    .replace_task_retention_policy(change)
                    .await)
            },
        )
        .await
    }

    async fn change_task_hold(
        &self,
        change: TaskHoldChange,
    ) -> Result<TaskControlRevision, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "change_task_hold",
            async { dispatch_backend!(self, |backend| backend.change_task_hold(change).await) },
        )
        .await
    }

    async fn select_expired_task_receipts(
        &self,
        request: ReceiptExpirySelection,
    ) -> Result<Option<ReceiptExpiryBatch>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "select_expired_task_receipts",
            async {
                dispatch_backend!(self, |backend| backend
                    .select_expired_task_receipts(request)
                    .await)
            },
        )
        .await
    }

    async fn expire_task_receipts(
        &self,
        request: ProtectedReceiptExpiry,
    ) -> Result<TaskControlRevision, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "expire_task_receipts",
            async {
                dispatch_backend!(self, |backend| backend.expire_task_receipts(request).await)
            },
        )
        .await
    }

    async fn prepare_task_details_purge(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "prepare_task_details_purge",
            async {
                dispatch_backend!(self, |backend| backend
                    .prepare_task_details_purge(selection)
                    .await)
            },
        )
        .await
    }

    async fn purge_task_details_batch(
        &self,
        request: ProtectedTaskPurge,
        budget: TaskPurgeBudget,
    ) -> Result<TaskPurgeProgress, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "purge_task_details_batch",
            async {
                dispatch_backend!(self, |backend| backend
                    .purge_task_details_batch(request, budget)
                    .await)
            },
        )
        .await
    }

    async fn list_pending_task_purges(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskPurgeTicket>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::TaskQueue,
            "list_pending_task_purges",
            async {
                dispatch_backend!(self, |backend| backend
                    .list_pending_task_purges(limit)
                    .await)
            },
        )
        .await
    }

    async fn reapply_task_retention(
        &self,
        request: ProtectedTaskReapplication,
        budget: TaskPurgeBudget,
    ) -> Result<TaskReapplicationProgress, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "reapply_task_retention",
            async {
                dispatch_backend!(self, |backend| backend
                    .reapply_task_retention(request, budget)
                    .await)
            },
        )
        .await
    }

    async fn list_legacy_task_receipts(
        &self,
        budget: TaskPurgeBudget,
    ) -> Result<Vec<TaskLegacyReceiptCandidate>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "list_legacy_task_receipts",
            async {
                dispatch_backend!(self, |backend| backend
                    .list_legacy_task_receipts(budget)
                    .await)
            },
        )
        .await
    }

    async fn backfill_task_receipts(
        &self,
        expected: TaskControlRevision,
        bindings: Vec<TaskLegacyReceiptBinding>,
    ) -> Result<TaskControlRevision, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::DeletionControl,
            "backfill_task_receipts",
            async {
                dispatch_backend!(self, |backend| backend
                    .backfill_task_receipts(expected, bindings)
                    .await)
            },
        )
        .await
    }
}
