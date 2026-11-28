//! In-memory ownership and acknowledgements for a restore drain generation.
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use hubuum_domain::{MaintenanceState, RestoreJobId};
use hubuum_storage_core::{
    StorageError, StorageRestoreCoordinatorSnapshot, StorageRestoreDrainState,
    StorageRestoreInstance,
};
use uuid::Uuid;

use crate::invalid_contract_value;

#[derive(Clone, Copy)]
struct Heartbeat {
    at: DateTime<Utc>,
    generation: i64,
    drained: bool,
}

#[derive(Clone, Default)]
pub(super) struct RestoreCoordinator {
    owner: Option<RestoreJobId>,
    generation: i64,
    instances: BTreeMap<Uuid, Heartbeat>,
}

impl RestoreCoordinator {
    pub(super) fn maintenance_state(&self) -> MaintenanceState {
        if self.owner.is_some() {
            MaintenanceState::Draining
        } else {
            MaintenanceState::Normal
        }
    }

    pub(super) fn snapshot(&self, now: DateTime<Utc>) -> StorageRestoreCoordinatorSnapshot {
        StorageRestoreCoordinatorSnapshot::new(self.maintenance_state(), self.owner, now)
    }

    pub(super) fn start(&mut self, job: RestoreJobId) -> Result<(), StorageError> {
        if self.owner.is_some() {
            return Err(StorageError::conflict(
                "Another maintenance operation is already active",
            ));
        }
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Restore maintenance generation exhausted"))?;
        self.owner = Some(job);
        Ok(())
    }

    pub(super) fn owns(&self, job: RestoreJobId) -> bool {
        self.owner == Some(job)
    }

    pub(super) fn resume(&mut self, owner: Option<RestoreJobId>) {
        if self.owner == owner {
            self.owner = None;
        }
    }

    pub(super) fn after_replay(&self, pending: bool) -> Self {
        Self {
            owner: self.owner.filter(|_| pending),
            generation: self.generation,
            ..Self::default()
        }
    }

    pub(super) fn heartbeat(
        &mut self,
        id: Uuid,
        now: DateTime<Utc>,
        is_idle: &(dyn Fn() -> bool + Send + Sync),
    ) {
        let drained = self.owner.is_some() && is_idle();
        self.instances.insert(
            id,
            Heartbeat {
                at: now,
                generation: self.generation,
                drained,
            },
        );
    }

    pub(super) fn drain_state(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        let instances = self
            .instances
            .iter()
            .filter(|(_, heartbeat)| heartbeat.at > cutoff)
            .map(|(id, heartbeat)| {
                StorageRestoreInstance::try_new(*id, heartbeat.generation, heartbeat.drained)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        StorageRestoreDrainState::try_new(self.generation, instances)
            .map_err(invalid_contract_value)
    }

    pub(super) fn remove_instance(&mut self, id: Uuid) {
        self.instances.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rstest::rstest;

    #[rstest]
    #[case::normal_operation(false)]
    #[case::previous_drain(true)]
    fn starting_a_drain_preserves_the_last_live_acknowledgement(#[case] previously_drained: bool) {
        let mut coordinator = RestoreCoordinator::default();
        let first_job = RestoreJobId::new(1).unwrap();
        if previously_drained {
            coordinator.start(first_job).unwrap();
        }
        let now = Utc::now();
        let cutoff = now - Duration::seconds(10);
        let id = Uuid::nil();
        coordinator.heartbeat(id, now, &|| true);
        let (previous_generation, previous_instances) =
            coordinator.drain_state(cutoff).unwrap().into_parts();
        assert_eq!(previous_instances.len(), 1);
        coordinator.resume(previously_drained.then_some(first_job));
        coordinator.start(RestoreJobId::new(2).unwrap()).unwrap();

        let (generation, instances) = coordinator.drain_state(cutoff).unwrap().into_parts();
        assert_ne!(generation, previous_generation);
        assert_eq!(instances, previous_instances);
    }

    #[test]
    fn exhausting_the_generation_preserves_existing_heartbeats_and_ownership() {
        let mut coordinator = RestoreCoordinator {
            generation: i64::MAX,
            ..RestoreCoordinator::default()
        };
        let now = Utc::now();
        coordinator.heartbeat(Uuid::nil(), now, &|| {
            panic!("normal operation does not drain")
        });
        let cutoff = now - Duration::seconds(10);
        let before = coordinator.drain_state(cutoff).unwrap().into_parts();
        assert!(coordinator.start(RestoreJobId::new(1).unwrap()).is_err());
        assert_eq!(coordinator.snapshot(now).restore_job_id(), None);
        assert_eq!(
            coordinator.drain_state(cutoff).unwrap().into_parts(),
            before
        );
    }

    #[test]
    fn a_different_job_cannot_release_the_current_drain() {
        let mut coordinator = RestoreCoordinator::default();
        let owner = RestoreJobId::new(1).unwrap();
        coordinator.start(owner).unwrap();
        coordinator.resume(Some(RestoreJobId::new(2).unwrap()));
        assert!(coordinator.owns(owner));
    }
}
