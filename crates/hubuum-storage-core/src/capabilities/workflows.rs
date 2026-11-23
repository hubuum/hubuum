pub use crate::backup_snapshot::*;
pub use crate::computed_fields::*;
pub use crate::export_query::*;
pub use crate::export_template_lifecycle::*;
pub use crate::import_workflow::*;
pub use crate::query_usage::*;
pub use crate::remote_target::*;
pub use crate::restore::*;
pub use crate::schema_evolution::*;
pub use crate::task_control::*;
pub use crate::task_execution::*;
pub use crate::task_queue::*;
pub use crate::task_search::StorageTaskListQuery;
pub use crate::{
    StorageAuthorizationPermission, StorageClass, StorageCollection, StorageObject, WorkflowStorage,
};
use crate::{StorageError, StorageMutationOutcome, StoragePage};
pub use crate::{StorageRestoreArtifactSummary, StorageRestoreArtifactSummaryParts};
use async_trait::async_trait;
use hubuum_domain::{ClassId, CollectionId, SchemaReference, TaskId};
use hubuum_events_core::EventContext;

#[async_trait]
pub trait TaskQueueStorage: Send + Sync {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError>;

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError>;

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StoragePage<StorageTask>, StorageError>;

    async fn list_task_events(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageTaskEvent>, StorageError>;

    async fn list_import_task_results(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageImportTaskResult>, StorageError>;

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError>;

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError>;

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError>;

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError>;

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError>;

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError>;
}

/// Mandatory worker state-machine behavior for every selectable backend.
#[async_trait]
pub trait TaskExecutionStorage: Send + Sync {
    /// Persist an authorized request; queued cancellation is terminal in the
    /// same transaction, while active execution retains its fenced lease.
    async fn request_task_cancellation(
        &self,
        request: StorageTaskCancellationRequest,
    ) -> Result<StorageTaskCancellationOutcome, StorageError>;

    /// Pin the first execution deadline using the adapter's trusted clock.
    async fn admit_task_execution(
        &self,
        request: StorageTaskExecutionAdmission,
    ) -> Result<StorageTaskExecutionObservation, StorageError>;

    /// Observe durable intent outside the executor's snapshot/transaction.
    async fn poll_task_execution(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTaskExecutionObservation, StorageError>;

    /// Finalize a stopped executor under its live claim, reconciling durable
    /// effects, results, cleanup and cancellation provenance atomically.
    async fn acknowledge_task_stop(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError>;

    /// Fence and persist outbound intent before performing an external effect.
    async fn begin_remote_dispatch(
        &self,
        request: StorageTaskRemoteDispatch,
    ) -> Result<(), StorageError>;

    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError>;

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError>;

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError>;

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError>;

    async fn update_task_state(
        &self,
        update: StorageTaskActiveUpdate,
    ) -> Result<StorageTask, StorageError>;

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError>;

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError>;

    async fn get_schema_profile_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageSchemaProfileArtifact>, StorageError>;

    async fn purge_expired_schema_profile_outputs(&self) -> Result<usize, StorageError>;

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError>;

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError>;
}

#[async_trait]
pub trait SchemaEvolutionStorage: Send + Sync {
    async fn get_schema_impact_boundary(
        &self,
        target: SchemaReference,
    ) -> Result<StorageSchemaImpactBoundary, StorageError>;
    async fn schema_compliance_counts(&self) -> Result<StorageComplianceCounts, StorageError>;
    async fn list_schema_revisions(
        &self,
        query: StorageSchemaPage,
    ) -> Result<Vec<StorageSchemaRevision>, StorageError>;
    async fn get_schema_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassSchemaState, StorageError>;
    async fn stage_schema_revision(
        &self,
        request: StorageSchemaStage,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError>;
    async fn abandon_schema_revision(
        &self,
        target: SchemaReference,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError>;
    async fn activate_schema_revision(
        &self,
        request: StorageSchemaActivation,
    ) -> Result<StorageMutationOutcome<StorageSchemaActivationResult>, StorageError>;
    async fn request_schema_work(
        &self,
        request: StorageSchemaWorkRequest,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError>;
    async fn get_schema_work(&self, task_id: TaskId) -> Result<StorageSchemaWork, StorageError>;
    /// Read one consistent checkpoint and its separately persisted findings.
    /// Charge the budget before retaining rows and abort with InputTooLarge on
    /// exhaustion. Use the shared report builder to preserve these semantics.
    async fn get_schema_work_report(
        &self,
        task_id: TaskId,
        budget: StorageSchemaReportBudget,
    ) -> Result<StorageSchemaWorkReport, StorageError>;
    /// Retain the latest complete rendering without changing the source analysis.
    async fn save_schema_repair_report(
        &self,
        request: StorageSchemaRepairReportWrite,
    ) -> Result<(), StorageError>;
    async fn get_schema_repair_report(
        &self,
        task_id: TaskId,
    ) -> Result<StorageSchemaRepairReport, StorageError>;
    async fn process_schema_work(
        &self,
        lease: StorageTaskLease,
        limits: StorageSchemaBatchLimits,
    ) -> Result<StorageSchemaWork, StorageError>;
    async fn cancel_schema_work(
        &self,
        task_id: TaskId,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError>;
    async fn list_schema_compliance(
        &self,
        query: StorageSchemaPage,
        status: Option<StorageComplianceStatus>,
    ) -> Result<Vec<StorageObjectCompliance>, StorageError>;
}
