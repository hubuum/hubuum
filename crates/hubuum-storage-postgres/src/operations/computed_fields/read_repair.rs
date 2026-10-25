//! Durable, globally bounded identity queue driven by the existing task worker.
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use diesel::prelude::*;
use diesel::sql_types::{Bool, Nullable, Timestamp};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, ObjectId};
use hubuum_storage_core::{StorageObject, StorageTask, StorageTaskKind, StorageTaskLease};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{insert_internal_task, locking};
use crate::operations::task_execution;
use crate::operations::task_rows::TaskRow;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

pub(crate) const PAYLOAD_TYPE: &str = "computed_read_repairs";
pub(crate) const MAX_QUEUED_REPAIRS: usize = 4096;
const ENQUEUE_DEADLINE: Duration = Duration::from_millis(25);
// Independent from class locks; held by admissions and each repair transaction.
const QUEUE_LOCK: i64 = 0x4855_4255_5245_5041;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComputedRepairIdentity {
    class_id: ClassId,
    object_id: ObjectId,
}

impl From<&StorageObject> for ComputedRepairIdentity {
    fn from(object: &StorageObject) -> Self {
        Self {
            class_id: object.class_id(),
            object_id: object.id(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepairQueue {
    #[serde(rename = "type")]
    payload_type: String,
    objects: Vec<ComputedRepairIdentity>,
}

impl RepairQueue {
    fn empty() -> Self {
        Self {
            payload_type: PAYLOAD_TYPE.into(),
            objects: Vec::new(),
        }
    }

    pub(super) fn from_task(task: &TaskRow) -> Result<Self, PostgresStorageError> {
        let queue: Self = serde_json::from_value(
            task.request_payload
                .clone()
                .ok_or_else(|| PostgresStorageError::database("Repair task payload is missing"))?,
        )
        .map_err(|error| PostgresStorageError::database(error.to_string()))?;
        let unique = queue
            .objects
            .iter()
            .map(|identity| identity.object_id)
            .collect::<BTreeSet<_>>();
        if queue.payload_type != PAYLOAD_TYPE
            || queue.objects.len() > MAX_QUEUED_REPAIRS
            || unique.len() != queue.objects.len()
        {
            return Err(PostgresStorageError::database(
                "Repair task queue is invalid",
            ));
        }
        Ok(queue)
    }

    pub(super) fn first_batch(&mut self, limit: usize) -> Option<(ClassId, Vec<i32>)> {
        self.objects
            .sort_unstable_by_key(|identity| (identity.class_id, identity.object_id));
        let class = self.objects.as_slice().first()?.class_id;
        let ids = self
            .objects
            .iter()
            .take_while(|identity| identity.class_id == class)
            .take(limit)
            .map(|identity| identity.object_id.id())
            .collect();
        Some((class, ids))
    }

    pub(super) fn remove_batch(&mut self, count: usize) {
        self.objects.drain(..count);
    }
    pub(super) fn len(&self) -> usize {
        self.objects.len()
    }
    pub(super) fn to_value(&self) -> Result<Value, PostgresStorageError> {
        serde_json::to_value(self)
            .map_err(|error| PostgresStorageError::internal(error.to_string()))
    }

    fn coalesce(&mut self, identities: Vec<ComputedRepairIdentity>) -> Admission {
        let mut objects = self
            .objects
            .drain(..)
            .map(|identity| (identity.object_id, identity))
            .collect::<BTreeMap<_, _>>();
        let mut admitted = 0;
        let mut coalesced = 0;
        let mut dropped = 0;
        for identity in identities {
            if let Some(stored) = objects.get_mut(&identity.object_id) {
                *stored = identity;
                coalesced += 1;
            } else if objects.len() < MAX_QUEUED_REPAIRS {
                objects.insert(identity.object_id, identity);
                admitted += 1;
            } else {
                dropped += 1;
            }
        }
        self.objects = objects.into_values().collect();
        Admission::Queued {
            admitted,
            coalesced,
            dropped,
            backlog: self.len(),
        }
    }
}

pub(crate) fn is_read_repair(task: &TaskRow) -> bool {
    task.kind == StorageTaskKind::Reindex.as_str()
        && task
            .request_payload
            .as_ref()
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str)
            == Some(PAYLOAD_TYPE)
}

pub(super) async fn lock_queue(
    connection: &mut PostgresConnection,
    wait: bool,
) -> Result<bool, PostgresStorageError> {
    if wait {
        diesel::sql_query("SELECT pg_advisory_xact_lock($1)")
            .bind::<diesel::sql_types::BigInt, _>(QUEUE_LOCK)
            .execute(connection)
            .await?;
        Ok(true)
    } else {
        diesel::select(
            diesel::dsl::sql::<Bool>("pg_try_advisory_xact_lock(")
                .bind::<diesel::sql_types::BigInt, _>(QUEUE_LOCK)
                .sql(")"),
        )
        .get_result::<bool>(connection)
        .await
        .map_err(PostgresStorageError::from)
    }
}

#[derive(Debug)]
enum Admission {
    Busy,
    Queued {
        admitted: usize,
        coalesced: usize,
        dropped: usize,
        backlog: usize,
    },
}

async fn admit(
    runtime: &PostgresRuntime,
    identities: Vec<ComputedRepairIdentity>,
) -> Result<Admission, PostgresStorageError> {
    crate::reach_fault_point(crate::PostgresFaultPoint::ComputedRepairBeforeEnqueue, None).await?;
    runtime
        .with_transaction(async move |connection| {
            if !lock_queue(connection, false).await? {
                return Ok(Admission::Busy);
            }
            use crate::schema::tasks::dsl as tasks;
            let existing = tasks::tasks
                .filter(tasks::kind.eq("reindex"))
                .filter(tasks::deleted_at.is_null())
                .filter(tasks::status.eq_any(["queued", "validating", "running"]))
                .filter(diesel::dsl::sql::<Bool>(
                    "request_payload->>'type' = 'computed_read_repairs'",
                ))
                .select(tasks::id)
                .first::<i32>(connection)
                .await
                .optional()?;
            let task = match existing {
                Some(id) => match tasks::tasks
                    .filter(tasks::id.eq(id))
                    .for_update()
                    .skip_locked()
                    .select(TaskRow::as_select())
                    .first::<TaskRow>(connection)
                    .await
                    .optional()?
                {
                    Some(task) => Some(task),
                    None => return Ok(Admission::Busy),
                },
                None => None,
            };
            let mut queue = match &task {
                Some(task) => RepairQueue::from_task(task)?,
                None => RepairQueue::empty(),
            };
            let admission = queue.coalesce(identities);
            let payload = queue.to_value()?;
            let total = task
                .as_ref()
                .map_or(0, |task| task.processed_items)
                .checked_add(queue.len() as i32)
                .ok_or_else(|| PostgresStorageError::database("Repair progress count overflow"))?;
            if let Some(task) = task {
                diesel::update(tasks::tasks.filter(tasks::id.eq(task.id)))
                    .set((
                        tasks::request_payload.eq(Some(payload)),
                        tasks::total_items.eq(total),
                        tasks::updated_at.eq(diesel::dsl::now),
                    ))
                    .execute(connection)
                    .await?;
            } else if !queue.objects.is_empty() {
                insert_internal_task(connection, StorageTaskKind::Reindex, payload, total, None)
                    .await?;
            }
            Ok::<_, PostgresStorageError>(admission)
        })
        .await
}

/// Admission is best effort. A lost acknowledgement may still have committed;
/// coalescing makes a later read safe in either case. No detached task is spawned.
#[tracing::instrument(name = "computed.repair.enqueue", skip_all, fields(object_count = identities.len()))]
pub(crate) async fn enqueue(runtime: &PostgresRuntime, identities: Vec<ComputedRepairIdentity>) {
    match tokio::time::timeout(ENQUEUE_DEADLINE, admit(runtime, identities)).await {
        Ok(Ok(Admission::Busy)) => runtime.record_computed_read_repair("queue_busy"),
        Ok(Ok(Admission::Queued {
            admitted,
            coalesced,
            dropped,
            backlog,
        })) => {
            runtime.record_computed_read_repair(if dropped > 0 {
                "queue_full"
            } else if admitted > 0 {
                "queued"
            } else {
                "coalesced"
            });
            tracing::debug!(
                admitted,
                coalesced,
                dropped,
                backlog,
                "computed repair queue admission"
            );
        }
        Ok(Err(error)) => {
            runtime.record_computed_read_repair("enqueue_failure");
            tracing::warn!(
                error_kind = error.kind().as_str(),
                "computed repair admission failed; a later stale read can retry"
            );
        }
        Err(_) => runtime.record_computed_read_repair("enqueue_timeout"),
    }
}

pub(super) enum BatchResult {
    Progress {
        repaired: usize,
        backlog: usize,
        errors: Vec<Vec<&'static str>>,
    },
    Complete(Box<TaskRow>),
}

pub(super) async fn execute(
    runtime: &PostgresRuntime,
    lease: StorageTaskLease,
) -> Result<StorageTask, PostgresStorageError> {
    loop {
        match locking::repair_queue_batch(runtime, lease.clone()).await {
            Ok(BatchResult::Progress {
                repaired,
                backlog,
                errors,
            }) => {
                for codes in errors {
                    runtime.record_computed_evaluation("shared", &codes);
                }
                runtime.record_computed_read_repair("success");
                tracing::debug!(repaired, backlog, "computed repair batch committed");
                tokio::task::yield_now().await;
            }
            Ok(BatchResult::Complete(task)) => {
                task_execution::record_task_terminal(runtime, &task);
                return task.into_storage();
            }
            Err(error) => {
                runtime.record_computed_read_repair("failure");
                return Err(error);
            }
        }
    }
}

pub(super) async fn persist_progress(
    connection: &mut PostgresConnection,
    claimed: task_execution::ClaimedTask,
    queue: &RepairQueue,
    handled: usize,
) -> Result<(), PostgresStorageError> {
    let task = task_execution::runnable_claimed_task(connection, claimed).await?;
    let processed = task
        .processed_items
        .checked_add(handled as i32)
        .ok_or_else(|| PostgresStorageError::database("Repair progress count overflow"))?;
    use crate::schema::tasks::dsl as tasks;
    let changed = diesel::update(
        tasks::tasks
            .filter(tasks::id.eq(claimed.id))
            .filter(tasks::lease_token.eq(Some(claimed.token)))
            .filter(
                tasks::lease_expires_at.gt(diesel::dsl::sql::<Nullable<Timestamp>>(
                    "clock_timestamp() AT TIME ZONE 'UTC'",
                )),
            ),
    )
    .set((
        tasks::request_payload.eq(Some(queue.to_value()?)),
        tasks::status.eq("running"),
        tasks::processed_items.eq(processed),
        tasks::success_items.eq(processed),
        tasks::summary.eq(Some(format!(
            "Computed read repair: {processed} identities handled, {} queued",
            queue.len()
        ))),
        tasks::updated_at.eq(diesel::dsl::now),
    ))
    .execute(connection)
    .await?;
    if changed != 1 {
        return Err(PostgresStorageError::conflict("Repair task claim expired"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn identity(class: i32, object: i32) -> ComputedRepairIdentity {
        ComputedRepairIdentity {
            class_id: ClassId::new(class).unwrap(),
            object_id: ObjectId::new(object).unwrap(),
        }
    }

    #[test]
    fn repeated_readers_coalesce_to_the_latest_class_identity() {
        let mut queue = RepairQueue::empty();
        queue.coalesce(vec![identity(1, 7), identity(2, 7), identity(2, 7)]);
        assert_eq!(queue.len(), 1);
        assert_eq!(
            queue.first_batch(10),
            Some((ClassId::new(2).unwrap(), vec![7]))
        );
    }

    #[rstest]
    #[case::one(1, vec![1])]
    #[case::two(2, vec![1, 2])]
    #[case::class_boundary(10, vec![1, 2])]
    fn batches_are_bounded_and_keep_classes_separate(
        #[case] limit: usize,
        #[case] expected: Vec<i32>,
    ) {
        let mut queue = RepairQueue::empty();
        queue.coalesce(vec![identity(2, 3), identity(1, 2), identity(1, 1)]);
        assert_eq!(
            queue.first_batch(limit),
            Some((ClassId::new(1).unwrap(), expected))
        );
    }

    #[test]
    fn full_queue_keeps_existing_work_and_coalesces_duplicates() {
        let mut queue = RepairQueue::empty();
        queue.coalesce(
            (1..=MAX_QUEUED_REPAIRS as i32)
                .map(|id| identity(1, id))
                .collect(),
        );
        let admission = queue.coalesce(vec![identity(2, 1), identity(1, 10000)]);
        assert!(matches!(
            admission,
            Admission::Queued {
                admitted: 0,
                coalesced: 1,
                dropped: 1,
                backlog: MAX_QUEUED_REPAIRS
            }
        ));
    }
}
