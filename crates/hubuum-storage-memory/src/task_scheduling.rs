use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use hubuum_domain::{PrincipalIdentity, TaskId};
use hubuum_storage_core::{
    StorageError, StorageTaskClaimRequest, StorageTaskKind, StorageTaskLease, StorageTaskStatus,
    TaskSchedulingMode, TaskSchedulingSnapshot, TaskSharedCapacityReason, TaskWorkerId,
    TaskWorkerKinds,
};

use super::MemoryState;

#[derive(Clone)]
struct Worker {
    kinds: TaskWorkerKinds,
    expires_at: DateTime<Utc>,
    claim: Option<(TaskId, String)>,
}

#[derive(Clone, Default)]
pub(super) struct MemoryTaskScheduler {
    workers: BTreeMap<TaskWorkerId, Worker>,
    served: BTreeMap<(&'static str, Option<PrincipalIdentity>), u64>,
    successes: u64,
    promotions: u64,
    last_promotion: u64,
    position: usize,
}

pub(super) struct ScheduledTask {
    id: TaskId,
    group: (&'static str, Option<PrincipalIdentity>),
    next_position: usize,
    promoted: bool,
}

impl ScheduledTask {
    pub(super) const fn id(&self) -> TaskId {
        self.id
    }
}

fn internal(kind: StorageTaskKind) -> bool {
    matches!(
        kind,
        StorageTaskKind::Reindex | StorageTaskKind::SchemaValidation
    )
}

impl MemoryTaskScheduler {
    pub(super) fn clear_workers(&mut self) {
        self.workers.clear();
    }

    pub(super) fn renew_worker(&mut self, lease: &StorageTaskLease, expires: DateTime<Utc>) {
        for worker in self.workers.values_mut() {
            if worker.claim.as_ref().is_some_and(|(id, token)| {
                *id == lease.task_id() && token == lease.token().adapter_value()
            }) {
                worker.expires_at = expires;
            }
        }
    }

    pub(super) fn commit_selection(&mut self, selection: ScheduledTask) {
        self.successes += 1;
        self.served.insert(selection.group, self.successes);
        if selection.promoted {
            self.promotions += 1;
            self.last_promotion = self.successes;
        } else {
            self.position = selection.next_position;
        }
    }

    pub(super) fn assign_worker(&mut self, id: TaskWorkerId, task: TaskId, token: String) {
        self.workers.get_mut(&id).expect("registered worker").claim = Some((task, token));
    }
}

impl MemoryState {
    pub(super) fn scheduling_snapshot(&self, now: DateTime<Utc>) -> TaskSchedulingSnapshot {
        let scheduler = &self.task_capacity.scheduler;
        let policy = self.scheduling_policy();
        let live: Vec<_> = scheduler
            .workers
            .values()
            .filter(|worker| worker.expires_at > now)
            .collect();
        let slots = (live.len() as u64).min(u64::from(policy.execution_slots()));
        let internal_slots = live
            .iter()
            .filter(|worker| worker.kinds.iter().any(internal))
            .count() as u64;
        let ordinary_slots = live
            .iter()
            .filter(|worker| worker.kinds.iter().any(|kind| !internal(kind)))
            .count();
        let reason = if policy.execution_slots() == 1 {
            Some(TaskSharedCapacityReason::SingleSlot)
        } else if slots <= u64::from(policy.internal_reserved_slots()) {
            Some(TaskSharedCapacityReason::InsufficientWorkers)
        } else if internal_slots < u64::from(policy.internal_reserved_slots()) {
            Some(TaskSharedCapacityReason::InsufficientInternalWorkers)
        } else if ordinary_slots == 0 {
            Some(TaskSharedCapacityReason::NoOrdinaryWorkers)
        } else {
            None
        };
        TaskSchedulingSnapshot::new(reason.map_or(TaskSchedulingMode::Reserved, |reason| {
            TaskSchedulingMode::Shared { reason }
        }))
        .with_pool(
            slots,
            internal_slots,
            StorageTaskKind::ALL
                .into_iter()
                .filter(|kind| !live.iter().any(|worker| worker.kinds.contains(*kind)))
                .collect(),
        )
        .with_progress(
            scheduler.successes,
            scheduler.promotions,
            scheduler.position as u64,
        )
    }

    fn worker_is_occupied(&self, worker: &Worker) -> bool {
        worker.claim.as_ref().is_some_and(|(id, token)| {
            self.tasks.get(&id.id()).is_some_and(|task| {
                task.status.is_active() && task.claim_token.as_ref() == Some(token)
            })
        })
    }

    pub(super) fn select_scheduled_task(
        &mut self,
        request: StorageTaskClaimRequest,
        now: DateTime<Utc>,
    ) -> Result<Option<ScheduledTask>, StorageError> {
        let expires_at = now
            .checked_add_signed(Duration::milliseconds(
                request.lease_duration().milliseconds(),
            ))
            .ok_or_else(|| StorageError::invalid_input("Worker lease is too large"))?;
        if self
            .task_capacity
            .scheduler
            .workers
            .get(&request.worker())
            .is_some_and(|worker| self.worker_is_occupied(worker))
        {
            return Ok(None);
        }
        let retained: BTreeMap<_, _> = self
            .task_capacity
            .scheduler
            .workers
            .iter()
            .filter(|(_, worker)| worker.expires_at > now || self.worker_is_occupied(worker))
            .map(|(id, worker)| (*id, worker.clone()))
            .collect();
        self.task_capacity.scheduler.workers = retained;
        self.task_capacity.scheduler.workers.insert(
            request.worker(),
            Worker {
                kinds: request.kinds(),
                expires_at,
                claim: None,
            },
        );
        let snapshot = self.scheduling_snapshot(now);
        let occupied: Vec<_> = self
            .tasks
            .values()
            .filter(|task| task.status.is_active() && task.claim_token.is_some())
            .collect();
        if occupied.len() as u64 >= snapshot.eligible_slots() {
            return Ok(None);
        }
        let ordinary_occupied = occupied.iter().filter(|task| !internal(task.kind)).count() as u64;
        let internal_capable_ordinary = self
            .task_capacity
            .scheduler
            .workers
            .values()
            .filter(|worker| {
                worker.expires_at > now
                    && worker.kinds.iter().any(internal)
                    && self.worker_is_occupied(worker)
                    && worker.claim.as_ref().is_some_and(|(id, _)| {
                        self.tasks
                            .get(&id.id())
                            .is_some_and(|task| !internal(task.kind))
                    })
            })
            .count() as u64;
        let internal_capable = self
            .task_capacity
            .scheduler
            .workers
            .values()
            .filter(|worker| worker.expires_at > now && worker.kinds.iter().any(internal))
            .count() as u64;
        let policy = self.scheduling_policy();
        let reserved = u64::from(policy.internal_reserved_slots());
        let ordinary_allowed = snapshot.mode() != TaskSchedulingMode::Reserved
            || (ordinary_occupied < snapshot.eligible_slots() - reserved
                && (!request.kinds().iter().any(internal)
                    || internal_capable_ordinary < internal_capable - reserved));
        let rotation = policy.rotation();
        let scheduler = &self.task_capacity.scheduler;
        if scheduler.successes == i64::MAX as u64 {
            return Err(StorageError::internal("Scheduling counter exhausted"));
        }
        let start = scheduler.position % rotation.len();
        let mut candidates = Vec::new();
        for task in self.tasks.values().filter(|task| {
            task.status == StorageTaskStatus::Queued && request.kinds().contains(task.kind)
        }) {
            if (!internal(task.kind) && !ordinary_allowed)
                || !self.task_claim_has_capacity(task.id)?
            {
                continue;
            }
            let group = (task.kind.as_str(), self.scheduling_principal(task.id)?);
            let distance = (0..rotation.len())
                .find(|offset| rotation[(start + offset) % rotation.len()] == task.kind)
                .expect("complete rotation");
            candidates.push((
                task.id,
                task.created_at,
                group,
                distance,
                scheduler.served.get(&group).copied().unwrap_or(0),
            ));
        }
        let promotion = scheduler.successes + 1 - scheduler.last_promotion
            >= u64::from(policy.promotion_interval());
        let overdue = now - Duration::seconds(i64::from(policy.overdue_seconds()));
        let promoted = if promotion {
            candidates
                .iter()
                .filter(|item| item.1 <= overdue)
                .min_by_key(|item| (item.1, item.0.id()))
        } else {
            None
        };
        let selected = promoted
            .or_else(|| {
                candidates
                    .iter()
                    .min_by_key(|item| (item.3, item.4, item.1, item.0.id()))
            })
            .copied();
        let Some((id, _, group, distance, _)) = selected else {
            return Ok(None);
        };
        Ok(Some(ScheduledTask {
            id,
            group,
            next_position: (start + distance + 1) % rotation.len(),
            promoted: promoted.is_some(),
        }))
    }
}
