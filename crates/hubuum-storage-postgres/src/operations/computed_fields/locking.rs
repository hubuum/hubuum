//! Transaction-owned protocol for computed backfills and read repair.
//!
//! Only this module can construct capabilities or access their connection. The
//! entry point owns the transaction; callers cannot commit, roll back, create a
//! savepoint, or acquire another class after locking objects. Dropping a token
//! does not unlock PostgreSQL rows: all locks last until transaction completion.

use super::read_repair::{self, BatchResult, RepairQueue};
use crate::operations::task_execution;
use crate::operations::task_rows::TaskRow;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    StorageTaskEventInput, StorageTaskLease, StorageTaskResultCounts, StorageTaskTerminalStatus,
    StorageTaskTerminalUpdate,
};

use super::{ComputedReindexPayload, ReindexBatch, ensure_computation_state};
use crate::operations::computed_materialization::{
    ObjectMaterializationInput, acquire_computed_class_shared_lock, rebuild_objects,
};
use crate::operations::object::ObjectRow;
use crate::operations::task_execution::{ClaimedTask, runnable_claimed_task};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError, SendAsyncFn};

/// Entry capability, constructed only inside a fresh transaction.
pub struct ComputedTransaction<'transaction> {
    connection: &'transaction mut PostgresConnection,
    batch_size: usize,
}

/// Proof that this transaction holds the shared definition advisory lock and
/// the class row's key-share lock for exactly `class_id`.
pub struct LockedComputedClass<'transaction> {
    transaction: ComputedTransaction<'transaction>,
    class_id: i32,
}

/// Objects loaded under row locks acquired after their class capability.
pub struct LockedComputedObjects<'transaction> {
    class: LockedComputedClass<'transaction>,
    rows: Vec<ObjectRow>,
}

/// The higher-ranked callback cannot return a capability borrowing this
/// transaction. No raw connection or transaction-control API is exposed.
pub async fn with_computed_transaction<F, R>(
    runtime: &PostgresRuntime,
    operation: F,
) -> Result<R, PostgresStorageError>
where
    F: for<'transaction> SendAsyncFn<
            ComputedTransaction<'transaction>,
            Result<R, PostgresStorageError>,
            Fut: Send,
        > + Send,
    R: Send,
{
    let batch_size = runtime.computed_reindex_batch_size();
    runtime
        .with_transaction(async move |connection| {
            operation(ComputedTransaction {
                connection,
                batch_size,
            })
            .await
        })
        .await
}

impl<'transaction> ComputedTransaction<'transaction> {
    #[cfg(feature = "integration-test-support")]
    pub async fn lock_class(
        self,
        class_id: i32,
    ) -> Result<LockedComputedClass<'transaction>, PostgresStorageError> {
        acquire_computed_class_shared_lock(self.connection, class_id).await?;
        self.lock_class_row(class_id).await
    }

    async fn lock_reindex_class(
        self,
        class_id: i32,
        claimed: ClaimedTask,
    ) -> Result<LockedComputedClass<'transaction>, PostgresStorageError> {
        acquire_computed_class_shared_lock(self.connection, class_id).await?;
        runnable_claimed_task(self.connection, claimed).await?;
        self.lock_class_row(class_id).await
    }

    async fn lock_class_row(
        self,
        class_id: i32,
    ) -> Result<LockedComputedClass<'transaction>, PostgresStorageError> {
        use crate::schema::hubuumclass::dsl as classes;
        classes::hubuumclass
            .filter(classes::id.eq(class_id))
            .for_key_share()
            .select(classes::id)
            .first::<i32>(self.connection)
            .await?;
        Ok(LockedComputedClass {
            transaction: self,
            class_id,
        })
    }
}

impl<'transaction> LockedComputedClass<'transaction> {
    /// Reload only this class's objects, in ascending ID order. Objects deleted
    /// or moved since the read snapshot are skipped, never materialized from
    /// stale source data or under a different class's proof.
    #[tracing::instrument(name = "computed.lock.objects", skip_all, fields(object_count = object_ids.len()))]
    pub async fn lock_objects(
        self,
        object_ids: &[i32],
    ) -> Result<LockedComputedObjects<'transaction>, PostgresStorageError> {
        if object_ids.len() > self.transaction.batch_size {
            return Err(PostgresStorageError::invalid_input(
                "Computed repair batch exceeds the configured batch size",
            ));
        }
        use crate::schema::hubuumobject::dsl as objects;
        let rows = objects::hubuumobject
            .filter(objects::hubuum_class_id.eq(self.class_id))
            .filter(objects::id.eq_any(object_ids))
            .order(objects::id.asc())
            .for_update()
            .select(ObjectRow::as_select())
            .load(self.transaction.connection)
            .await?;
        Ok(LockedComputedObjects { class: self, rows })
    }

    async fn lock_reindex_objects(
        self,
        cursor: i32,
        upper_bound: i32,
    ) -> Result<LockedComputedObjects<'transaction>, PostgresStorageError> {
        let batch_size = i64::try_from(self.transaction.batch_size).map_err(|_| {
            PostgresStorageError::invalid_input(
                "computed reindex batch size exceeds the supported range",
            )
        })?;
        use crate::schema::hubuumobject::dsl as objects;
        let rows = objects::hubuumobject
            .filter(objects::hubuum_class_id.eq(self.class_id))
            .filter(objects::id.gt(cursor))
            .filter(objects::id.le(upper_bound))
            .order(objects::id.asc())
            .limit(batch_size)
            .for_update()
            .select(ObjectRow::as_select())
            .load(self.transaction.connection)
            .await?;
        Ok(LockedComputedObjects { class: self, rows })
    }
}

impl LockedComputedObjects<'_> {
    pub async fn repair(self) -> Result<Vec<Vec<&'static str>>, PostgresStorageError> {
        let connection = self.class.transaction.connection;
        let state = ensure_computation_state(connection, self.class.class_id).await?;
        let inputs = self
            .rows
            .iter()
            .map(|row| ObjectMaterializationInput::new(row.id, self.class.class_id, &row.data))
            .collect::<Vec<_>>();
        let evaluations = rebuild_objects(
            connection,
            self.class.class_id,
            state.evaluation_revision,
            &inputs,
        )
        .await?;
        Ok(evaluations
            .into_iter()
            .map(|evaluation| evaluation.error_codes().to_vec())
            .collect())
    }

    async fn rebuild(
        self,
        claimed: ClaimedTask,
        target_revision: i64,
    ) -> Result<ReindexBatch, PostgresStorageError> {
        let connection = self.class.transaction.connection;
        let state = ensure_computation_state(connection, self.class.class_id).await?;
        if state.evaluation_revision != target_revision || state.active_task_id != Some(claimed.id)
        {
            return Ok(ReindexBatch::Superseded);
        }
        let Some(last_id) = self.rows.last().map(|row| row.id) else {
            return Ok(ReindexBatch::Complete);
        };
        let inputs = self
            .rows
            .iter()
            .map(|row| ObjectMaterializationInput::new(row.id, self.class.class_id, &row.data))
            .collect::<Vec<_>>();
        let summaries =
            rebuild_objects(connection, self.class.class_id, target_revision, &inputs).await?;
        runnable_claimed_task(connection, claimed).await?;
        Ok(ReindexBatch::Rows {
            last_id,
            count: i32::try_from(self.rows.len()).unwrap_or(i32::MAX),
            error_codes: summaries
                .into_iter()
                .map(|summary| summary.error_codes().to_vec())
                .collect(),
        })
    }
}

pub(super) async fn reindex_batch(
    runtime: &PostgresRuntime,
    claimed: ClaimedTask,
    payload: &ComputedReindexPayload,
    cursor: i32,
) -> Result<ReindexBatch, PostgresStorageError> {
    with_computed_transaction(runtime, async move |transaction| {
        transaction
            .lock_reindex_class(payload.class_id, claimed)
            .await?
            .lock_reindex_objects(cursor, payload.object_upper_bound)
            .await?
            .rebuild(claimed, payload.target_revision)
            .await
    })
    .await
}

/// The queue driver owns the transaction, queue serialization lock, and claim.
/// Class-before-task-before-object ordering is the same as the rebuild protocol.
#[tracing::instrument(name = "computed.repair", skip_all)]
pub(super) async fn repair_queue_batch(
    runtime: &PostgresRuntime,
    lease: StorageTaskLease,
) -> Result<BatchResult, PostgresStorageError> {
    let claimed = task_execution::claimed_task(&lease)?;
    let batch_size = runtime.computed_reindex_batch_size();
    runtime
        .with_transaction(async move |connection| {
            read_repair::lock_queue(connection, true).await?;
            let task = crate::schema::tasks::table
                .find(claimed.id)
                .select(TaskRow::as_select())
                .first::<TaskRow>(connection)
                .await?;
            let mut queue = RepairQueue::from_task(&task)?;
            let Some((class_id, ids)) = queue.first_batch(batch_size) else {
                task_execution::runnable_claimed_task(connection, claimed).await?;
                let counts = crate::validate_persisted(
                    "repair progress",
                    StorageTaskResultCounts::try_new(
                        task.processed_items,
                        task.success_items,
                        task.failed_items,
                    ),
                )?;
                let completed = task_execution::complete_task_on_connection(
                    connection,
                    StorageTaskTerminalUpdate::new(
                        lease,
                        StorageTaskTerminalStatus::Succeeded,
                        counts,
                    )
                    .summary(Some("Computed read repairs completed".into()))
                    .started_at(task.started_at.map(|value| value.and_utc())),
                    StorageTaskEventInput::new("succeeded", "Computed read repairs completed"),
                )
                .await?;
                return Ok(BatchResult::Complete(Box::new(completed)));
            };
            acquire_computed_class_shared_lock(connection, class_id.id()).await?;
            task_execution::runnable_claimed_task(connection, claimed).await?;
            let class_exists = crate::schema::hubuumclass::table
                .find(class_id.id())
                .for_key_share()
                .select(crate::schema::hubuumclass::id)
                .first::<i32>(connection)
                .await
                .optional()?
                .is_some();
            let errors = if class_exists {
                let class = LockedComputedClass {
                    transaction: ComputedTransaction {
                        connection,
                        batch_size,
                    },
                    class_id: class_id.id(),
                };
                class.lock_objects(&ids).await?.repair().await?
            } else {
                Vec::new()
            };
            crate::reach_fault_point(
                crate::PostgresFaultPoint::ComputedRepairBeforeCommit,
                Some(connection),
            )
            .await?;
            queue.remove_batch(ids.len());
            read_repair::persist_progress(connection, claimed, &queue, ids.len()).await?;
            Ok::<_, PostgresStorageError>(BatchResult::Progress {
                repaired: errors.len(),
                backlog: queue.len(),
                errors,
            })
        })
        .await
}
