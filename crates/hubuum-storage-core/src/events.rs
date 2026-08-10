use async_trait::async_trait;
use hubuum_domain::{EventFanoutSettings, EventRetentionSettings};
use hubuum_events_core::EventEnvelope;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use crate::StorageError;

/// Atomic event-to-delivery fan-out required from every storage backend.
///
/// The implementation owns claiming, subscription matching, delivery-row
/// insertion, claim release, and worker notification as one backend operation.
#[async_trait]
pub trait EventFanoutStorage: Send + Sync {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<usize, StorageError>;
}

/// Opaque ownership proof for an in-flight event delivery.
///
/// Consumers return this value when acknowledging success or failure. The
/// claim token is intentionally private and redacted from diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct EventDeliveryClaim {
    delivery_id: i64,
    attempts: i32,
    token: Uuid,
}

impl EventDeliveryClaim {
    #[must_use]
    pub const fn new(delivery_id: i64, attempts: i32, token: Uuid) -> Self {
        Self {
            delivery_id,
            attempts,
            token,
        }
    }

    #[must_use]
    pub const fn delivery_id(&self) -> i64 {
        self.delivery_id
    }

    #[must_use]
    pub const fn attempts(&self) -> i32 {
        self.attempts
    }

    #[must_use]
    pub const fn token(&self) -> Uuid {
        self.token
    }
}

impl std::fmt::Debug for EventDeliveryClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventDeliveryClaim")
            .field("delivery_id", &self.delivery_id)
            .field("attempts", &self.attempts)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Sink settings required by a delivery transport.
#[derive(Clone, PartialEq, Eq)]
pub struct EventDeliverySink {
    id: i32,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
}

impl EventDeliverySink {
    #[must_use]
    pub fn new(
        id: i32,
        name: impl Into<String>,
        kind: impl Into<String>,
        configuration: Value,
        secret_ref: Option<String>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            kind: kind.into(),
            configuration,
            secret_ref,
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
    pub const fn configuration(&self) -> &Value {
        &self.configuration
    }

    #[must_use]
    pub fn secret_ref(&self) -> Option<&str> {
        self.secret_ref.as_deref()
    }
}

impl std::fmt::Debug for EventDeliverySink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventDeliverySink")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("configuration", &"<redacted>")
            .field(
                "secret_ref",
                &self.secret_ref.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Subscription routing settings required by a delivery transport.
#[derive(Clone, PartialEq, Eq)]
pub struct EventDeliverySubscription {
    id: i32,
    name: String,
    routing: Value,
}

impl EventDeliverySubscription {
    #[must_use]
    pub fn new(id: i32, name: impl Into<String>, routing: Value) -> Self {
        Self {
            id,
            name: name.into(),
            routing,
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
    pub const fn routing(&self) -> &Value {
        &self.routing
    }
}

impl std::fmt::Debug for EventDeliverySubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventDeliverySubscription")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("routing", &"<redacted>")
            .finish()
    }
}

/// Complete, backend-neutral work item handed to the delivery application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventDeliveryWorkItem {
    claim: EventDeliveryClaim,
    envelope: EventEnvelope,
    subscription: EventDeliverySubscription,
    sink: EventDeliverySink,
}

impl EventDeliveryWorkItem {
    #[must_use]
    pub const fn new(
        claim: EventDeliveryClaim,
        envelope: EventEnvelope,
        subscription: EventDeliverySubscription,
        sink: EventDeliverySink,
    ) -> Self {
        Self {
            claim,
            envelope,
            subscription,
            sink,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        EventDeliveryClaim,
        EventEnvelope,
        EventDeliverySubscription,
        EventDeliverySink,
    ) {
        (self.claim, self.envelope, self.subscription, self.sink)
    }
}

/// One claim operation and its earliest scheduled retry, if no rows were due.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventDeliveryBatch {
    deliveries: Vec<EventDeliveryWorkItem>,
    next_wakeup_in: Option<Duration>,
}

impl EventDeliveryBatch {
    #[must_use]
    pub const fn new(
        deliveries: Vec<EventDeliveryWorkItem>,
        next_wakeup_in: Option<Duration>,
    ) -> Self {
        Self {
            deliveries,
            next_wakeup_in,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<EventDeliveryWorkItem>, Option<Duration>) {
        (self.deliveries, self.next_wakeup_in)
    }
}

/// Claim and acknowledgement lifecycle required from every storage backend.
///
/// The claim operation must atomically choose due work, mark it in flight, and
/// return fully enriched delivery DTOs. Acknowledgements must verify the opaque
/// claim so a stale worker cannot overwrite a newer attempt.
#[async_trait]
pub trait EventDeliveryStorage: Send + Sync {
    async fn claim_event_delivery_batch(
        &self,
        settings: hubuum_domain::EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError>;

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &EventDeliveryClaim,
    ) -> Result<(), StorageError>;

    async fn mark_event_delivery_failed(
        &self,
        claim: &EventDeliveryClaim,
        settings: hubuum_domain::EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError>;
}

/// Serialized event selected for archival before retention purges it.
///
/// The storage adapter owns the persistence model and converts it to this
/// transport-neutral JSON document. Consumers can persist the document
/// without depending on a backend row type or inspecting claim metadata.
#[derive(Clone, PartialEq, Eq)]
pub struct RetainedEvent {
    id: i64,
    json: String,
}

impl RetainedEvent {
    #[must_use]
    pub fn new(id: i64, json: impl Into<String>) -> Self {
        Self {
            id,
            json: json.into(),
        }
    }

    #[must_use]
    pub const fn id(&self) -> i64 {
        self.id
    }

    #[must_use]
    pub fn json(&self) -> &str {
        &self.json
    }
}

impl std::fmt::Debug for RetainedEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedEvent")
            .field("id", &self.id)
            .field("json", &"<redacted>")
            .finish()
    }
}

/// Application-owned destination used during an atomic retention operation.
///
/// Returning an error instructs the adapter to roll back the purge. The
/// adapter must not retain this reference or invoke it after the operation
/// returns.
pub trait EventArchive: Send + Sync {
    fn archive(&self, events: &[RetainedEvent]) -> Result<(), StorageError>;
}

/// Counts produced by one bounded retention operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventRetentionSummary {
    purged_events: usize,
    purged_terminal_deliveries: usize,
}

impl EventRetentionSummary {
    #[must_use]
    pub const fn new(purged_events: usize, purged_terminal_deliveries: usize) -> Self {
        Self {
            purged_events,
            purged_terminal_deliveries,
        }
    }

    #[must_use]
    pub const fn purged_events(self) -> usize {
        self.purged_events
    }

    #[must_use]
    pub const fn purged_terminal_deliveries(self) -> usize {
        self.purged_terminal_deliveries
    }

    #[must_use]
    pub const fn did_work(self) -> bool {
        self.purged_events > 0 || self.purged_terminal_deliveries > 0
    }
}

/// Atomic archival and purge behavior required from every storage backend.
///
/// Implementations must coordinate competing workers, select a bounded batch,
/// invoke `archive` before deletion, roll back when archival fails, purge only
/// eligible events and terminal deliveries, and commit those actions as one
/// backend transaction.
#[async_trait]
pub trait EventRetentionStorage: Send + Sync {
    async fn process_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
        archive: &dyn EventArchive,
    ) -> Result<EventRetentionSummary, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_dto_debug_output_redacts_claim_and_transport_secrets() {
        let token = Uuid::new_v4();
        let claim = EventDeliveryClaim::new(7, 2, token);
        let sink = EventDeliverySink::new(
            8,
            "webhook",
            "webhook",
            serde_json::json!({"authorization": "config-secret"}),
            Some("secret-reference".to_string()),
        );
        let subscription = EventDeliverySubscription::new(
            9,
            "subscription",
            serde_json::json!({"url": "https://routing-secret.invalid"}),
        );

        let debug = format!("{claim:?} {sink:?} {subscription:?}");

        assert!(!debug.contains(&token.to_string()));
        assert!(!debug.contains("config-secret"));
        assert!(!debug.contains("secret-reference"));
        assert!(!debug.contains("routing-secret"));
    }

    #[test]
    fn retained_event_debug_output_redacts_the_serialized_payload() {
        let event = RetainedEvent::new(11, r#"{"metadata":"payload-secret"}"#);

        let debug = format!("{event:?}");

        assert!(debug.contains("11"));
        assert!(!debug.contains("payload-secret"));
    }

    #[test]
    fn retention_summary_reports_whether_the_backend_did_work() {
        assert!(!EventRetentionSummary::default().did_work());
        assert!(EventRetentionSummary::new(1, 0).did_work());
        assert!(EventRetentionSummary::new(0, 1).did_work());
    }
}
