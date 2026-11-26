use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use hubuum_domain::{EventDeliveryId, EventDeliveryStatus, EventSubscriptionId};
use hubuum_storage_core::capabilities::{
    common::StorageError,
    events::{StorageEventDelivery, StorageEventDeliveryClaim},
};
use uuid::Uuid;

use crate::support::invalid_contract_value;

#[derive(Default, Clone)]
pub(super) struct EventDeliveries {
    entries: BTreeMap<EventDeliveryId, DeliveryEntry>,
}

#[derive(Clone)]
struct DeliveryEntry {
    delivery: StorageEventDelivery,
    claim: Option<Uuid>,
}

impl EventDeliveries {
    pub(super) fn values(&self) -> impl DoubleEndedIterator<Item = &StorageEventDelivery> {
        self.entries.values().map(|entry| &entry.delivery)
    }

    pub(super) fn get(&self, id: EventDeliveryId) -> Option<&StorageEventDelivery> {
        self.entries.get(&id).map(|entry| &entry.delivery)
    }

    pub(super) fn store_unclaimed(&mut self, delivery: StorageEventDelivery) {
        self.entries.insert(
            delivery.id(),
            DeliveryEntry {
                delivery,
                claim: None,
            },
        );
    }

    pub(super) fn store_claimed(&mut self, delivery: StorageEventDelivery, token: Uuid) {
        self.entries.insert(
            delivery.id(),
            DeliveryEntry {
                delivery,
                claim: Some(token),
            },
        );
    }

    pub(super) fn claimed(
        &self,
        claim: &StorageEventDeliveryClaim,
    ) -> Result<&StorageEventDelivery, StorageError> {
        let entry = self
            .entries
            .get(&claim.delivery_id())
            .ok_or_else(|| StorageError::not_found("Delivery was removed"))?;
        if entry.claim != Some(claim.token())
            || entry.delivery.status() != EventDeliveryStatus::InFlight
        {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        Ok(&entry.delivery)
    }

    pub(super) fn claimed_attempt(
        &self,
        claim: &StorageEventDeliveryClaim,
    ) -> Result<&StorageEventDelivery, StorageError> {
        self.claimed(claim)
            .ok()
            .filter(|delivery| {
                delivery.attempts() == claim.attempts()
                    && delivery
                        .locked_until()
                        .is_some_and(|deadline| deadline > Utc::now())
            })
            .ok_or_else(|| StorageError::conflict("Event delivery claim is stale"))
    }

    pub(super) fn remove_where(
        &mut self,
        predicate: impl Fn(&StorageEventDelivery) -> bool,
    ) -> usize {
        let before = self.entries.len();
        self.entries.retain(|_, entry| !predicate(&entry.delivery));
        before - self.entries.len()
    }

    pub(super) fn reschedule_deferred(
        &mut self,
        subscriptions: &HashSet<EventSubscriptionId>,
        eligible: DateTime<Utc>,
        reason: Option<&str>,
    ) -> Result<(), StorageError> {
        for delivery in self
            .entries
            .values_mut()
            .map(|entry| &mut entry.delivery)
            .filter(|delivery| {
                subscriptions.contains(&delivery.subscription_id())
                    && delivery.status() == EventDeliveryStatus::Pending
                    && delivery.deferred_reason().is_some()
            })
        {
            *delivery = with_deferred_reason(
                &rebuild_event_delivery(
                    delivery,
                    delivery.status(),
                    delivery.attempts(),
                    eligible,
                    delivery.last_error().map(str::to_owned),
                    delivery.locked_until(),
                )?,
                reason.map(str::to_owned),
            )?;
        }
        Ok(())
    }
}

pub(super) fn rebuild_event_delivery(
    delivery: &StorageEventDelivery,
    status: EventDeliveryStatus,
    attempts: i32,
    next_attempt_at: DateTime<Utc>,
    last_error: Option<String>,
    locked_until: Option<DateTime<Utc>>,
) -> Result<StorageEventDelivery, StorageError> {
    StorageEventDelivery::builder(
        delivery.id(),
        delivery.event_id(),
        delivery.subscription_id(),
        status,
        next_attempt_at,
        delivery.created_at(),
        Utc::now(),
    )
    .purpose(delivery.purpose())
    .deferred_reason(delivery.deferred_reason().map(str::to_owned))
    .attempts(attempts)
    .last_error(last_error)
    .locked_until(locked_until)
    .try_build()
    .map_err(invalid_contract_value)
}

pub(super) fn with_deferred_reason(
    current: &StorageEventDelivery,
    deferred_reason: Option<String>,
) -> Result<StorageEventDelivery, StorageError> {
    StorageEventDelivery::builder(
        current.id(),
        current.event_id(),
        current.subscription_id(),
        current.status(),
        current.next_attempt_at(),
        current.created_at(),
        current.updated_at(),
    )
    .attempts(current.attempts())
    .last_error(current.last_error().map(str::to_owned))
    .locked_until(current.locked_until())
    .purpose(current.purpose())
    .deferred_reason(deferred_reason)
    .try_build()
    .map_err(invalid_contract_value)
}
