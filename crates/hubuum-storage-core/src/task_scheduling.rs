//! Durable start fairness and leased worker availability. Scheduling does not
//! promise CPU shares or preemption of a task that already owns execution.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{StorageTaskKind, StorageTaskLeaseDuration, StorageValidationError};

/// One execution slot, unique for the lifetime of a worker loop. Reuse this
/// value on every poll; creating a new identity on each poll invents capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskWorkerId(Uuid);

impl TaskWorkerId {
    #[must_use]
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    #[must_use]
    pub fn as_string(self) -> String {
        self.0.to_string()
    }
}

/// Validated, nonempty set of task kinds supported by an execution slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskWorkerKinds(u8);

impl TaskWorkerKinds {
    pub const ALL: Self = Self(127);

    pub fn new(kinds: &[StorageTaskKind]) -> Result<Self, StorageValidationError> {
        if kinds.is_empty() {
            return Err(StorageValidationError::invalid(
                "A worker must support at least one task kind",
            ));
        }
        let mut bits = 0;
        for kind in kinds {
            let mask = 1 << kind_index(*kind);
            if bits & mask != 0 {
                return Err(StorageValidationError::invalid(
                    "Worker task kinds must be unique",
                ));
            }
            bits |= mask;
        }
        Ok(Self(bits))
    }

    #[must_use]
    pub fn contains(self, kind: StorageTaskKind) -> bool {
        self.0 & (1 << kind_index(kind)) != 0
    }

    pub fn iter(self) -> impl Iterator<Item = StorageTaskKind> {
        StorageTaskKind::ALL
            .into_iter()
            .filter(move |kind| self.contains(*kind))
    }
}

/// A claim also renews this worker's availability using the backend clock and
/// the requested lease duration. A successful task heartbeat renews it too.
#[derive(Clone, Copy, Debug)]
pub struct StorageTaskClaimRequest {
    worker: TaskWorkerId,
    kinds: TaskWorkerKinds,
    lease_duration: StorageTaskLeaseDuration,
}

impl StorageTaskClaimRequest {
    #[must_use]
    pub const fn new(worker: TaskWorkerId, lease_duration: StorageTaskLeaseDuration) -> Self {
        Self {
            worker,
            kinds: TaskWorkerKinds::ALL,
            lease_duration,
        }
    }

    #[must_use]
    pub const fn with_kinds(mut self, kinds: TaskWorkerKinds) -> Self {
        self.kinds = kinds;
        self
    }

    #[must_use]
    pub const fn worker(self) -> TaskWorkerId {
        self.worker
    }
    #[must_use]
    pub const fn kinds(self) -> TaskWorkerKinds {
        self.kinds
    }
    #[must_use]
    pub const fn lease_duration(self) -> StorageTaskLeaseDuration {
        self.lease_duration
    }
}

fn kind_index(kind: StorageTaskKind) -> usize {
    StorageTaskKind::ALL
        .iter()
        .position(|entry| *entry == kind)
        .expect("complete task kinds")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WeightWire")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskKindWeight {
    kind: StorageTaskKind,
    weight: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WeightWire {
    kind: StorageTaskKind,
    weight: u8,
}

impl TryFrom<WeightWire> for TaskKindWeight {
    type Error = StorageValidationError;
    fn try_from(value: WeightWire) -> Result<Self, Self::Error> {
        Self::new(value.kind, value.weight)
    }
}

impl TaskKindWeight {
    pub fn new(kind: StorageTaskKind, weight: u8) -> Result<Self, StorageValidationError> {
        if !(1..=32).contains(&weight) {
            return Err(StorageValidationError::invalid(
                "Task class weight must be between 1 and 32",
            ));
        }
        Ok(Self { kind, weight })
    }
    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }
    #[must_use]
    pub const fn weight(self) -> u8 {
        self.weight
    }
}

/// Complete scheduling policy, persisted with admission in one revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SchedulingWire")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskSchedulingPolicy {
    execution_slots: u16,
    internal_reserved_slots: u16,
    promotion_interval: u16,
    overdue_seconds: u32,
    weights: [TaskKindWeight; 7],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchedulingWire {
    execution_slots: u16,
    internal_reserved_slots: u16,
    promotion_interval: u16,
    overdue_seconds: u32,
    weights: [TaskKindWeight; 7],
}

impl TryFrom<SchedulingWire> for TaskSchedulingPolicy {
    type Error = StorageValidationError;
    fn try_from(value: SchedulingWire) -> Result<Self, Self::Error> {
        if StorageTaskKind::ALL
            .iter()
            .any(|kind| value.weights.iter().filter(|w| w.kind == *kind).count() != 1)
        {
            return Err(StorageValidationError::invalid(
                "Scheduling weights must contain every task kind exactly once",
            ));
        }
        let mut policy = Self::default()
            .with_capacity(value.execution_slots, value.internal_reserved_slots)?
            .with_age_promotion(value.promotion_interval, value.overdue_seconds)?;
        policy.weights = value.weights;
        Ok(policy)
    }
}

impl Default for TaskSchedulingPolicy {
    fn default() -> Self {
        Self {
            execution_slots: 64,
            internal_reserved_slots: 1,
            promotion_interval: 8,
            overdue_seconds: 300,
            weights: StorageTaskKind::ALL.map(|kind| TaskKindWeight { kind, weight: 1 }),
        }
    }
}

impl TaskSchedulingPolicy {
    pub fn with_capacity(
        mut self,
        slots: u16,
        reserved: u16,
    ) -> Result<Self, StorageValidationError> {
        if !(1..=1024).contains(&slots)
            || reserved == 0
            || reserved > slots
            || (slots > 1 && reserved == slots)
        {
            return Err(StorageValidationError::invalid(
                "Scheduling requires 1–1024 slots and an internal reservation leaving at least one ordinary slot; one slot uses shared mode",
            ));
        }
        self.execution_slots = slots;
        self.internal_reserved_slots = reserved;
        Ok(self)
    }

    pub fn with_age_promotion(
        mut self,
        interval: u16,
        seconds: u32,
    ) -> Result<Self, StorageValidationError> {
        if !(2..=1024).contains(&interval) || !(1..=604_800).contains(&seconds) {
            return Err(StorageValidationError::invalid(
                "Age promotion requires an interval of 2–1024 successful claims and an age of 1–604800 seconds",
            ));
        }
        self.promotion_interval = interval;
        self.overdue_seconds = seconds;
        Ok(self)
    }

    #[must_use]
    pub fn with_weight(mut self, weight: TaskKindWeight) -> Self {
        *self
            .weights
            .iter_mut()
            .find(|entry| entry.kind == weight.kind)
            .expect("complete weights") = weight;
        self
    }
    #[must_use]
    pub const fn execution_slots(&self) -> u16 {
        self.execution_slots
    }
    #[must_use]
    pub const fn internal_reserved_slots(&self) -> u16 {
        self.internal_reserved_slots
    }
    #[must_use]
    pub const fn promotion_interval(&self) -> u16 {
        self.promotion_interval
    }
    #[must_use]
    pub const fn overdue_seconds(&self) -> u32 {
        self.overdue_seconds
    }
    /// Canonical ordering makes JSON member ordering irrelevant to fairness.
    #[must_use]
    pub fn rotation(&self) -> Vec<StorageTaskKind> {
        StorageTaskKind::ALL
            .into_iter()
            .flat_map(|kind| {
                let weight = self
                    .weights
                    .iter()
                    .find(|entry| entry.kind == kind)
                    .expect("complete weights")
                    .weight;
                std::iter::repeat_n(kind, usize::from(weight))
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum TaskSharedCapacityReason {
    SingleSlot,
    InsufficientWorkers,
    InsufficientInternalWorkers,
    NoOrdinaryWorkers,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum TaskSchedulingMode {
    Reserved,
    Shared { reason: TaskSharedCapacityReason },
}

/// Aggregate diagnostics only; worker and principal identities stay private.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskSchedulingSnapshot {
    mode: TaskSchedulingMode,
    eligible_slots: u64,
    internal_capable_slots: u64,
    unsupported_kinds: Vec<StorageTaskKind>,
    successful_claims: u64,
    promoted_claims: u64,
    ordinary_position: u64,
}

impl TaskSchedulingSnapshot {
    #[must_use]
    pub fn new(mode: TaskSchedulingMode) -> Self {
        Self {
            mode,
            eligible_slots: 0,
            internal_capable_slots: 0,
            unsupported_kinds: Vec::new(),
            successful_claims: 0,
            promoted_claims: 0,
            ordinary_position: 0,
        }
    }
    #[must_use]
    pub fn with_pool(
        mut self,
        eligible: u64,
        internal: u64,
        unsupported: Vec<StorageTaskKind>,
    ) -> Self {
        self.eligible_slots = eligible;
        self.internal_capable_slots = internal;
        self.unsupported_kinds = unsupported;
        self
    }
    #[must_use]
    pub const fn with_progress(mut self, successes: u64, promotions: u64, position: u64) -> Self {
        self.successful_claims = successes;
        self.promoted_claims = promotions;
        self.ordinary_position = position;
        self
    }
    #[must_use]
    pub const fn mode(&self) -> TaskSchedulingMode {
        self.mode
    }
    #[must_use]
    pub const fn eligible_slots(&self) -> u64 {
        self.eligible_slots
    }
    #[must_use]
    pub const fn successful_claims(&self) -> u64 {
        self.successful_claims
    }
    #[must_use]
    pub const fn promoted_claims(&self) -> u64 {
        self.promoted_claims
    }
    #[must_use]
    pub const fn ordinary_position(&self) -> u64 {
        self.ordinary_position
    }
    #[must_use]
    pub fn unsupported_kinds(&self) -> &[StorageTaskKind] {
        &self.unsupported_kinds
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::single(1, 1, true)]
    #[case::ordinary_slot(2, 1, true)]
    #[case::all_reserved(2, 2, false)]
    #[case::no_workers(0, 1, false)]
    #[case::no_reservation(4, 0, false)]
    #[case::overflow(1025, 1, false)]
    fn capacity_requires_a_viable_reservation(
        #[case] slots: u16,
        #[case] reserved: u16,
        #[case] valid: bool,
    ) {
        assert_eq!(
            TaskSchedulingPolicy::default()
                .with_capacity(slots, reserved)
                .is_ok(),
            valid
        );
    }

    #[rstest]
    #[case::every_claim(1, 300, false)]
    #[case::bounded(8, 300, true)]
    #[case::unbounded(1025, 300, false)]
    #[case::no_age(8, 0, false)]
    fn promotion_must_leave_ordinary_turns(
        #[case] interval: u16,
        #[case] age: u32,
        #[case] valid: bool,
    ) {
        assert_eq!(
            TaskSchedulingPolicy::default()
                .with_age_promotion(interval, age)
                .is_ok(),
            valid
        );
    }

    #[rstest]
    #[case::zero(0, false)]
    #[case::one(1, true)]
    #[case::maximum(32, true)]
    #[case::over_maximum(33, false)]
    fn class_weights_are_bounded(#[case] weight: u8, #[case] valid: bool) {
        assert_eq!(
            TaskKindWeight::new(StorageTaskKind::Import, weight).is_ok(),
            valid
        );
    }

    #[test]
    fn duplicate_weights_cannot_cross_the_policy_boundary() {
        let mut wire = serde_json::to_value(TaskSchedulingPolicy::default()).unwrap();
        wire["weights"][1] = wire["weights"][0].clone();
        assert!(serde_json::from_value::<TaskSchedulingPolicy>(wire).is_err());
    }

    #[test]
    fn wire_order_does_not_change_rotation() {
        let policy = TaskSchedulingPolicy::default()
            .with_weight(TaskKindWeight::new(StorageTaskKind::Export, 3).unwrap());
        let mut wire = serde_json::to_value(&policy).unwrap();
        wire["weights"].as_array_mut().unwrap().reverse();
        assert_eq!(
            serde_json::from_value::<TaskSchedulingPolicy>(wire)
                .unwrap()
                .rotation(),
            policy.rotation()
        );
    }

    #[rstest]
    #[case::empty(vec![],false)]
    #[case::duplicate(vec![StorageTaskKind::Import,StorageTaskKind::Import],false)]
    #[case::restricted(vec![StorageTaskKind::Reindex],true)]
    fn worker_eligibility_is_a_nonempty_set(
        #[case] kinds: Vec<StorageTaskKind>,
        #[case] valid: bool,
    ) {
        assert_eq!(TaskWorkerKinds::new(&kinds).is_ok(), valid);
    }
}
