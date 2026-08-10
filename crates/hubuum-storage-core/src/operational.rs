use async_trait::async_trait;
use hubuum_domain::{MaintenanceState, TokenRetentionSettings};

use crate::StorageError;

/// Backend-neutral readiness data used by probes and orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    schema_ready: bool,
    maintenance_state: MaintenanceState,
}

impl ReadinessSnapshot {
    #[must_use]
    pub const fn new(schema_ready: bool, maintenance_state: MaintenanceState) -> Self {
        Self {
            schema_ready,
            maintenance_state,
        }
    }

    #[must_use]
    pub const fn schema_is_ready(self) -> bool {
        self.schema_ready
    }

    #[must_use]
    pub const fn maintenance_state(self) -> MaintenanceState {
        self.maintenance_state
    }
}

/// Persisted delivery status counts, independent of an API or database row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventDeliveryStatusSnapshot {
    total: i64,
    pending: i64,
    in_flight: i64,
    succeeded: i64,
    failed: i64,
    dead: i64,
    retryable: i64,
}

impl EventDeliveryStatusSnapshot {
    #[must_use]
    pub const fn new(
        total: i64,
        pending: i64,
        in_flight: i64,
        succeeded: i64,
        failed: i64,
        dead: i64,
        retryable: i64,
    ) -> Self {
        Self {
            total,
            pending,
            in_flight,
            succeeded,
            failed,
            dead,
            retryable,
        }
    }

    #[must_use]
    pub const fn total(self) -> i64 {
        self.total
    }

    #[must_use]
    pub const fn pending(self) -> i64 {
        self.pending
    }

    #[must_use]
    pub const fn in_flight(self) -> i64 {
        self.in_flight
    }

    #[must_use]
    pub const fn succeeded(self) -> i64 {
        self.succeeded
    }

    #[must_use]
    pub const fn failed(self) -> i64 {
        self.failed
    }

    #[must_use]
    pub const fn dead(self) -> i64 {
        self.dead
    }

    #[must_use]
    pub const fn retryable(self) -> i64 {
        self.retryable
    }
}

/// Persisted fan-out queue health without application worker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventFanoutSnapshot {
    pending_events: i64,
    in_flight_events: i64,
    stale_claims: i64,
    oldest_pending_age_seconds: Option<i64>,
}

impl EventFanoutSnapshot {
    #[must_use]
    pub const fn new(
        pending_events: i64,
        in_flight_events: i64,
        stale_claims: i64,
        oldest_pending_age_seconds: Option<i64>,
    ) -> Self {
        Self {
            pending_events,
            in_flight_events,
            stale_claims,
            oldest_pending_age_seconds,
        }
    }

    #[must_use]
    pub const fn pending_events(self) -> i64 {
        self.pending_events
    }

    #[must_use]
    pub const fn in_flight_events(self) -> i64 {
        self.in_flight_events
    }

    #[must_use]
    pub const fn stale_claims(self) -> i64 {
        self.stale_claims
    }

    #[must_use]
    pub const fn oldest_pending_age_seconds(self) -> Option<i64> {
        self.oldest_pending_age_seconds
    }
}

/// Persisted delivery queue health without application worker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventQueueSnapshot {
    counts: EventDeliveryStatusSnapshot,
    stale_claims: i64,
    oldest_due_age_seconds: Option<i64>,
}

impl EventQueueSnapshot {
    #[must_use]
    pub const fn new(
        counts: EventDeliveryStatusSnapshot,
        stale_claims: i64,
        oldest_due_age_seconds: Option<i64>,
    ) -> Self {
        Self {
            counts,
            stale_claims,
            oldest_due_age_seconds,
        }
    }

    #[must_use]
    pub const fn counts(self) -> EventDeliveryStatusSnapshot {
        self.counts
    }

    #[must_use]
    pub const fn stale_claims(self) -> i64 {
        self.stale_claims
    }

    #[must_use]
    pub const fn oldest_due_age_seconds(self) -> Option<i64> {
        self.oldest_due_age_seconds
    }
}

/// Backend-neutral identity and configuration of an event sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSinkSnapshot {
    id: i32,
    name: String,
    kind: String,
    enabled: bool,
}

impl EventSinkSnapshot {
    #[must_use]
    pub fn new(id: i32, name: String, kind: String, enabled: bool) -> Self {
        Self {
            id,
            name,
            kind,
            enabled,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
}

/// Persisted queue health grouped by sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSinkHealthSnapshot {
    sink: EventSinkSnapshot,
    subscription_count: i64,
    queue: EventQueueSnapshot,
}

impl EventSinkHealthSnapshot {
    #[must_use]
    pub const fn new(
        sink: EventSinkSnapshot,
        subscription_count: i64,
        queue: EventQueueSnapshot,
    ) -> Self {
        Self {
            sink,
            subscription_count,
            queue,
        }
    }

    #[must_use]
    pub const fn sink(&self) -> &EventSinkSnapshot {
        &self.sink
    }

    #[must_use]
    pub const fn subscription_count(&self) -> i64 {
        self.subscription_count
    }

    #[must_use]
    pub const fn queue(&self) -> EventQueueSnapshot {
        self.queue
    }
}

/// Persisted queue health grouped by subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSubscriptionHealthSnapshot {
    id: i32,
    name: String,
    collection_id: i32,
    enabled: bool,
    sink: EventSinkSnapshot,
    queue: EventQueueSnapshot,
}

impl EventSubscriptionHealthSnapshot {
    #[must_use]
    pub fn new(
        id: i32,
        name: String,
        collection_id: i32,
        enabled: bool,
        sink: EventSinkSnapshot,
        queue: EventQueueSnapshot,
    ) -> Self {
        Self {
            id,
            name,
            collection_id,
            enabled,
            sink,
            queue,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    #[must_use]
    pub const fn sink(&self) -> &EventSinkSnapshot {
        &self.sink
    }

    #[must_use]
    pub const fn queue(&self) -> EventQueueSnapshot {
        self.queue
    }
}

/// Complete persisted event-pipeline health snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDeliveryHealthSnapshot {
    fanout: EventFanoutSnapshot,
    delivery: EventQueueSnapshot,
    sinks: Vec<EventSinkHealthSnapshot>,
    subscriptions: Vec<EventSubscriptionHealthSnapshot>,
}

impl EventDeliveryHealthSnapshot {
    #[must_use]
    pub const fn new(
        fanout: EventFanoutSnapshot,
        delivery: EventQueueSnapshot,
        sinks: Vec<EventSinkHealthSnapshot>,
        subscriptions: Vec<EventSubscriptionHealthSnapshot>,
    ) -> Self {
        Self {
            fanout,
            delivery,
            sinks,
            subscriptions,
        }
    }

    #[must_use]
    pub const fn fanout(&self) -> EventFanoutSnapshot {
        self.fanout
    }

    #[must_use]
    pub const fn delivery(&self) -> EventQueueSnapshot {
        self.delivery
    }

    #[must_use]
    pub fn sinks(&self) -> &[EventSinkHealthSnapshot] {
        &self.sinks
    }

    #[must_use]
    pub fn subscriptions(&self) -> &[EventSubscriptionHealthSnapshot] {
        &self.subscriptions
    }
}

/// Operational state every selectable storage backend must expose.
#[async_trait]
pub trait OperationalStateStorage: Send + Sync {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError>;

    async fn maintenance_state(&self) -> Result<MaintenanceState, StorageError>;
}

/// Token retention behavior required from every selectable storage backend.
#[async_trait]
pub trait TokenRetentionStorage: Send + Sync {
    async fn purge_expired_tokens(
        &self,
        settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError>;
}

/// Event-pipeline persistence health required from every selectable backend.
#[async_trait]
pub trait EventHealthStorage: Send + Sync {
    async fn event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError>;
}
