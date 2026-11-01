use async_trait::async_trait;
use diesel::sql_types::{Bool, Integer};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::*;

use super::PostgresStorage;
use crate::PostgresStorageError;
use crate::operations::task_retention::{self as retention, authority};

#[async_trait]
impl TaskRetentionStorage for PostgresStorage {
    async fn read_task_receipt_activation(&self) -> Result<TaskReceiptActivation, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| {
                Ok::<_, PostgresStorageError>(retention::read_on(connection).await?.activation)
            })
            .await
            .map_err(StorageError::from)
    }

    async fn list_pending_task_admissions(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskAdmissionTicket>, StorageError> {
        if limit == 0 || limit as usize > TASK_CONTROL_MAX_PURGE_TASKS {
            return Err(StorageError::invalid_input(
                "Pending admission list requires 1..=64 tickets",
            ));
        }
        self.runtime()
            .with_control_connection(async move |connection| {
                let rows = diesel::sql_query(
                    "SELECT public.hubuum_task_admissions_pending($1) AS material",
                )
                .bind::<Integer, _>(limit as i32)
                .load::<retention::MaterialRow>(connection)
                .await?;
                rows.into_iter()
                    .map(|row| retention::decode(row.material))
                    .collect::<Result<Vec<_>, PostgresStorageError>>()
            })
            .await
            .map_err(StorageError::from)
    }
    async fn read_task_control(&self) -> Result<TaskControlState, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| authority::state_on(connection).await)
            .await
            .map_err(StorageError::from)
    }

    async fn capture_control_checkpoint(&self) -> Result<ControlCheckpointCapture, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| authority::capture_on(connection).await)
            .await
            .map_err(StorageError::from)
    }

    async fn commit_task_control(
        &self,
        change: TaskControlChange,
    ) -> Result<TaskControlState, StorageError> {
        self.runtime().with_control_transaction(async move |connection| {
            authority::lock_expected_on(connection,change.expected()).await?;
            if change.next().receipts()!=change.expected().receipts()
                || change.next().purge_batches()!=change.expected().purge_batches()
                || change.next().holds()!=change.expected().holds() {
                return Err(PostgresStorageError::invalid_input("Use the validated receipt, hold or managed purge operation for protected records"));
            }
            if change.next().activation().is_protected() && !change.expected().activation().is_protected() {
                let remaining=diesel::select(diesel::dsl::sql::<Bool>("EXISTS(SELECT 1 FROM public.tasks WHERE idempotency_key IS NOT NULL AND receipt_generation IS NULL)")).get_result::<bool>(connection).await?;
                if remaining { return Err(PostgresStorageError::conflict("Legacy task receipt backfill is incomplete")); }
                let pending=diesel::select(diesel::dsl::sql::<Bool>("EXISTS(SELECT 1 FROM public.tasks WHERE receipt_legacy AND receipt_generation IS NOT NULL AND receipt_checkpoint IS NULL AND status IN ('queued','validating','running'))")).get_result::<bool>(connection).await?;
                if pending { return Err(PostgresStorageError::conflict("Legacy task receipt activation is incomplete")); }
            }
            authority::store_state_on(connection,change.next()).await?;
            Ok::<_,PostgresStorageError>(change.next().clone())
        }).await.map_err(StorageError::from)
    }

    async fn recover_task_control(
        &self,
        change: TaskControlRecovery,
    ) -> Result<TaskControlRevision, StorageError> {
        self.runtime().with_control_transaction(async move |connection| {
            diesel::sql_query("SELECT id FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE").execute(connection).await?;
            authority::lock_expected_on(connection,change.expected()).await?;
            let exposed=diesel::select(diesel::dsl::sql::<Bool>("public.hubuum_deletion_exposure_allowed()")).get_result::<bool>(connection).await?;
            if exposed { return Err(PostgresStorageError::conflict("Application recovery must quarantine the incarnation before task recovery")); }
            authority::store_state_on(connection,change.merged()).await?;
            diesel::sql_query("UPDATE hubuum_control.task_retention_control SET reapplied_incarnation=NULL,reapplied_revision=NULL WHERE id=1").execute(connection).await?;
            Ok::<_,PostgresStorageError>(change.merged().revision())
        }).await.map_err(StorageError::from)
    }

    async fn read_task_retention_policy(&self) -> Result<TaskRetentionPolicy, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| {
                retention::read_on(connection).await?.policy.ok_or_else(|| {
                    PostgresStorageError::database("Task retention policy is missing")
                })
            })
            .await
            .map_err(StorageError::from)
    }

    async fn replace_task_retention_policy(
        &self,
        change: TaskRetentionPolicyChange,
    ) -> Result<TaskControlRevision, StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
                let header = retention::lock_on(connection).await?;
                if header.policy.as_ref().map(TaskRetentionPolicy::revision)
                    != Some(change.expected())
                {
                    return Err(PostgresStorageError::conflict(
                        "Task retention policy changed",
                    ));
                }
                let previous = authority::state_on(connection).await?;
                let next = previous.with_policy(change.policy().clone())?;
                authority::store_state_on(connection, &next).await?;
                Ok::<_, PostgresStorageError>(next.revision())
            })
            .await
            .map_err(StorageError::from)
    }

    async fn change_task_hold(
        &self,
        change: TaskHoldChange,
    ) -> Result<TaskControlRevision, StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
                retention::lock_on(connection).await?;
                let previous = authority::state_on(connection).await?;
                if previous.holds().get(&change.hold().id()) != change.expected() {
                    return Err(PostgresStorageError::conflict("Task hold decision changed"));
                }
                let next = previous.with_hold(change.hold().clone())?;
                authority::store_state_on(connection, &next).await?;
                Ok::<_, PostgresStorageError>(next.revision())
            })
            .await
            .map_err(StorageError::from)
    }

    async fn select_expired_task_receipts(
        &self,
        request: ReceiptExpirySelection,
    ) -> Result<Option<ReceiptExpiryBatch>, StorageError> {
        retention::select_expired_task_receipts(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
    async fn expire_task_receipts(
        &self,
        request: ProtectedReceiptExpiry,
    ) -> Result<TaskControlRevision, StorageError> {
        retention::expire_task_receipts(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
    async fn prepare_task_details_purge(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError> {
        retention::prepare_task_details_purge(self.runtime(), selection)
            .await
            .map_err(StorageError::from)
    }
    async fn prepare_expired_task_outputs(
        &self,
        selection: TaskPurgeSelection,
    ) -> Result<Option<TaskPurgeTicket>, StorageError> {
        retention::prepare_expired_task_outputs(self.runtime(), selection)
            .await
            .map_err(StorageError::from)
    }
    async fn purge_task_details_batch(
        &self,
        request: ProtectedTaskPurge,
        budget: TaskPurgeBudget,
    ) -> Result<TaskPurgeProgress, StorageError> {
        retention::purge_task_details_batch(self.runtime(), request, budget)
            .await
            .map_err(StorageError::from)
    }
    async fn list_pending_task_purges(
        &self,
        limit: u32,
    ) -> Result<Vec<TaskPurgeTicket>, StorageError> {
        retention::list_pending_task_purges(self.runtime(), limit)
            .await
            .map_err(StorageError::from)
    }
    async fn reapply_task_retention(
        &self,
        request: ProtectedTaskReapplication,
        budget: TaskPurgeBudget,
    ) -> Result<TaskReapplicationProgress, StorageError> {
        retention::reapply_task_retention(self.runtime(), request, budget)
            .await
            .map_err(StorageError::from)
    }
    async fn list_legacy_task_receipts(
        &self,
        budget: TaskPurgeBudget,
    ) -> Result<Vec<TaskLegacyReceiptCandidate>, StorageError> {
        retention::list_legacy_task_receipts(self.runtime(), budget)
            .await
            .map_err(StorageError::from)
    }
    async fn backfill_task_receipts(
        &self,
        expected: TaskControlRevision,
        bindings: Vec<TaskLegacyReceiptBinding>,
    ) -> Result<TaskControlRevision, StorageError> {
        retention::backfill_task_receipts(self.runtime(), expected, bindings)
            .await
            .map_err(StorageError::from)
    }
}
