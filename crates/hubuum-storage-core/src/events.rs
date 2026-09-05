use async_trait::async_trait;
use hubuum_domain::{
    CollectionId, EventDeliveryId, EventFanoutSettings, EventRetentionSettings, EventSinkId,
    EventSubscriptionId, ResourceRevision,
};
use hubuum_events_core::{EventEnvelope, EventId, EventSequence, TraceLink};
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

use crate::{StorageAuditReceipt, StorageError, StorageValidationError};

/// One committed event returned by a storage adapter after an append or read.
///
/// The envelope and revisions are backend-neutral, validated domain values.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageRecordedEvent {
    envelope: EventEnvelope,
    before_revision: Option<ResourceRevision>,
    after_revision: Option<ResourceRevision>,
}

impl StorageRecordedEvent {
    #[must_use]
    pub const fn new(
        envelope: EventEnvelope,
        before_revision: Option<ResourceRevision>,
        after_revision: Option<ResourceRevision>,
    ) -> Self {
        Self {
            envelope,
            before_revision,
            after_revision,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        EventEnvelope,
        Option<ResourceRevision>,
        Option<ResourceRevision>,
    ) {
        (self.envelope, self.before_revision, self.after_revision)
    }

    /// Reduce the committed event to the non-sensitive proof returned by
    /// ordinary mutation APIs.
    #[must_use]
    pub fn into_audit_receipt(self) -> StorageAuditReceipt {
        let (envelope, before_revision, after_revision) = self.into_parts();
        StorageAuditReceipt::new(
            envelope.id(),
            EventId::from(envelope.event_id()),
            envelope.entity_type(),
            envelope.action(),
            before_revision,
            after_revision,
        )
    }
}

/// Atomic event-to-delivery fan-out required from every storage backend.
///
/// The implementation owns claiming, subscription matching, delivery-row
/// insertion, claim release, and worker notification as one backend operation.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventFanoutOutcome {
    processed: usize,
    trace_links: Vec<TraceLink>,
}

impl StorageEventFanoutOutcome {
    #[must_use]
    pub const fn new(processed: usize, trace_links: Vec<TraceLink>) -> Self {
        Self {
            processed,
            trace_links,
        }
    }

    #[must_use]
    pub const fn processed(&self) -> usize {
        self.processed
    }

    #[must_use]
    pub fn trace_links(&self) -> &[TraceLink] {
        &self.trace_links
    }
}

impl std::fmt::Debug for StorageEventFanoutOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageEventFanoutOutcome")
            .field("processed", &self.processed)
            .field("trace_link_count", &self.trace_links.len())
            .finish()
    }
}

#[async_trait]
pub trait EventFanoutStorage: Send + Sync {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<StorageEventFanoutOutcome, StorageError>;
}

/// Opaque ownership proof for an in-flight event delivery.
///
/// Consumers return this value when acknowledging success or failure. The
/// claim token is intentionally private and redacted from diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventDeliveryClaim {
    delivery_id: EventDeliveryId,
    attempts: i32,
    token: Uuid,
}

impl StorageEventDeliveryClaim {
    pub fn try_new(
        delivery_id: EventDeliveryId,
        attempts: i32,
        token: Uuid,
    ) -> Result<Self, StorageValidationError> {
        if attempts < 0 {
            return Err(StorageValidationError::invalid(
                "An event delivery claim cannot have a negative attempt count",
            ));
        }
        Ok(Self {
            delivery_id,
            attempts,
            token,
        })
    }

    #[must_use]
    pub const fn delivery_id(&self) -> EventDeliveryId {
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

impl std::fmt::Debug for StorageEventDeliveryClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageEventDeliveryClaim")
            .field("delivery_id", &self.delivery_id)
            .field("attempts", &self.attempts)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// Sink settings required by a delivery transport.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageEventDeliverySink {
    id: EventSinkId,
    name: String,
    kind: String,
    configuration: Value,
    secret_ref: Option<String>,
}

impl StorageEventDeliverySink {
    pub fn try_new(
        id: EventSinkId,
        name: impl Into<String>,
        kind: impl Into<String>,
        configuration: Value,
        secret_ref: Option<String>,
    ) -> Result<Self, StorageValidationError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(StorageValidationError::invalid(
                "Event delivery sink name must not be empty",
            ));
        }
        let kind = kind.into();
        if kind.trim().is_empty() {
            return Err(StorageValidationError::invalid(
                "Event delivery sink kind must not be empty",
            ));
        }
        if !configuration.is_object() {
            return Err(StorageValidationError::invalid(
                "Event delivery sink configuration must be a JSON object",
            ));
        }
        if secret_ref
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(StorageValidationError::invalid(
                "Event delivery sink secret_ref must not be empty",
            ));
        }
        Ok(Self {
            id,
            name,
            kind,
            configuration,
            secret_ref,
        })
    }

    #[must_use]
    pub const fn id(&self) -> EventSinkId {
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

impl std::fmt::Debug for StorageEventDeliverySink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageEventDeliverySink")
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
pub struct StorageEventDeliverySubscription {
    id: EventSubscriptionId,
    collection_id: CollectionId,
    name: String,
    routing: Value,
}

impl StorageEventDeliverySubscription {
    pub fn try_new(
        id: EventSubscriptionId,
        collection_id: CollectionId,
        name: impl Into<String>,
        routing: Value,
    ) -> Result<Self, StorageValidationError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(StorageValidationError::invalid(
                "Event delivery subscription name must not be empty",
            ));
        }
        if !routing.is_object() {
            return Err(StorageValidationError::invalid(
                "Event delivery subscription routing must be a JSON object",
            ));
        }
        Ok(Self {
            id,
            collection_id,
            name,
            routing,
        })
    }

    #[must_use]
    pub const fn id(&self) -> EventSubscriptionId {
        self.id
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
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

impl std::fmt::Debug for StorageEventDeliverySubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageEventDeliverySubscription")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("routing", &"<redacted>")
            .finish()
    }
}

/// Complete, backend-neutral work item handed to the delivery application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageEventDeliveryWorkItem {
    claim: StorageEventDeliveryClaim,
    envelope: EventEnvelope,
    subscription: StorageEventDeliverySubscription,
    sink: StorageEventDeliverySink,
}

impl StorageEventDeliveryWorkItem {
    #[must_use]
    pub const fn new(
        claim: StorageEventDeliveryClaim,
        envelope: EventEnvelope,
        subscription: StorageEventDeliverySubscription,
        sink: StorageEventDeliverySink,
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
        StorageEventDeliveryClaim,
        EventEnvelope,
        StorageEventDeliverySubscription,
        StorageEventDeliverySink,
    ) {
        (self.claim, self.envelope, self.subscription, self.sink)
    }
}

/// One claim operation and its earliest scheduled retry, if no rows were due.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageEventDeliveryBatch {
    deliveries: Vec<StorageEventDeliveryWorkItem>,
    next_wakeup_in: Option<Duration>,
}

impl StorageEventDeliveryBatch {
    #[must_use]
    pub const fn new(
        deliveries: Vec<StorageEventDeliveryWorkItem>,
        next_wakeup_in: Option<Duration>,
    ) -> Self {
        Self {
            deliveries,
            next_wakeup_in,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StorageEventDeliveryWorkItem>, Option<Duration>) {
        (self.deliveries, self.next_wakeup_in)
    }
}

/// Claim and acknowledgement lifecycle required from every storage backend.
///
/// The claim operation must atomically choose due work, mark it in flight, and
/// return fully enriched, validated delivery DTOs. Invalid persisted envelope,
/// sink, subscription, or claim values are backend failures and must roll back
/// the claim. Acknowledgements must verify the opaque claim so a stale worker
/// cannot overwrite a newer attempt.
#[async_trait]
pub trait EventDeliveryWorkerStorage: Send + Sync {
    async fn claim_event_delivery_batch(
        &self,
        settings: hubuum_domain::EventDeliverySettings,
    ) -> Result<StorageEventDeliveryBatch, StorageError>;

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &StorageEventDeliveryClaim,
    ) -> Result<(), StorageError>;

    async fn mark_event_delivery_failed(
        &self,
        claim: &StorageEventDeliveryClaim,
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
pub struct StorageRetainedEvent {
    id: EventSequence,
    json: String,
}

impl StorageRetainedEvent {
    pub fn try_new(
        id: EventSequence,
        json: impl Into<String>,
    ) -> Result<Self, StorageValidationError> {
        let json = json.into();
        let document: serde_json::Value = serde_json::from_str(&json).map_err(|error| {
            StorageValidationError::invalid(format!("Retained event is not valid JSON: {error}"))
        })?;
        if !document.is_object() {
            return Err(StorageValidationError::invalid(
                "Retained event JSON must be an object",
            ));
        }
        Ok(Self { id, json })
    }

    #[must_use]
    pub const fn id(&self) -> EventSequence {
        self.id
    }

    #[must_use]
    pub fn json(&self) -> &str {
        &self.json
    }
}

impl std::fmt::Debug for StorageRetainedEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageRetainedEvent")
            .field("id", &self.id)
            .field("json", &"<redacted>")
            .finish()
    }
}

/// Stable identifier for a durably claimed event-retention batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StorageEventRetentionBatchId(Uuid);

impl StorageEventRetentionBatchId {
    #[must_use]
    pub const fn new(id: Uuid) -> Self {
        Self(id)
    }

    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// A durably claimed, retryable batch of events awaiting archival and purge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageEventRetentionBatch {
    id: StorageEventRetentionBatchId,
    events: Vec<StorageRetainedEvent>,
}

impl StorageEventRetentionBatch {
    #[must_use]
    pub const fn new(id: StorageEventRetentionBatchId, events: Vec<StorageRetainedEvent>) -> Self {
        Self { id, events }
    }

    #[must_use]
    pub const fn id(&self) -> StorageEventRetentionBatchId {
        self.id
    }

    #[must_use]
    pub fn events(&self) -> &[StorageRetainedEvent] {
        &self.events
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Application-owned destination for durably claimed retention batches.
///
/// Implementations must be idempotent by [`StorageEventRetentionBatch::id`]. A
/// retry of the same batch must succeed without duplicating archived events.
#[async_trait]
pub trait EventArchiveSink: Send + Sync {
    async fn archive(&self, batch: &StorageEventRetentionBatch) -> Result<(), StorageError>;
}

/// Counts produced by one bounded retention operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageEventRetentionSummary {
    purged_events: usize,
    purged_terminal_deliveries: usize,
}

impl StorageEventRetentionSummary {
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

/// Durable claim/archive/ack behavior required from every storage backend.
///
/// Claims survive process failure. Completing a claim must be idempotent and
/// purge only the exact eligible events represented by that claim, plus the
/// bounded terminal-delivery work captured with it.
#[async_trait]
pub trait EventRetentionStorage: Send + Sync {
    async fn claim_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
    ) -> Result<Option<StorageEventRetentionBatch>, StorageError>;

    async fn complete_event_retention_batch(
        &self,
        batch_id: StorageEventRetentionBatchId,
    ) -> Result<StorageEventRetentionSummary, StorageError>;
}

/// Execute the application-owned claim/archive/ack protocol.
///
/// Keeping this orchestration outside [`EventRetentionStorage`] prevents an
/// adapter from replacing the ordering guarantee that archival completes
/// before the durable claim is acknowledged and purged.
pub async fn execute_event_retention_batch<S>(
    storage: &S,
    settings: EventRetentionSettings,
    archive: &dyn EventArchiveSink,
) -> Result<StorageEventRetentionSummary, StorageError>
where
    S: EventRetentionStorage + ?Sized,
{
    let Some(batch) = storage.claim_event_retention_batch(settings).await? else {
        return Ok(StorageEventRetentionSummary::default());
    };
    if !batch.is_empty() {
        archive.archive(&batch).await?;
    }
    storage.complete_event_retention_batch(batch.id()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_dto_debug_output_redacts_claim_and_transport_secrets() {
        let token = Uuid::new_v4();
        let claim =
            StorageEventDeliveryClaim::try_new(EventDeliveryId::new(7).unwrap(), 2, token).unwrap();
        let sink = StorageEventDeliverySink::try_new(
            EventSinkId::new(8).unwrap(),
            "webhook",
            "webhook",
            serde_json::json!({"authorization": "config-secret"}),
            Some("secret-reference".to_string()),
        )
        .unwrap();
        let subscription = StorageEventDeliverySubscription::try_new(
            EventSubscriptionId::new(9).unwrap(),
            hubuum_domain::CollectionId::new(1).unwrap(),
            "subscription",
            serde_json::json!({"url": "https://routing-secret.invalid"}),
        )
        .unwrap();

        let debug = format!("{claim:?} {sink:?} {subscription:?}");

        assert!(!debug.contains(&token.to_string()));
        assert!(!debug.contains("config-secret"));
        assert!(!debug.contains("secret-reference"));
        assert!(!debug.contains("routing-secret"));
    }

    #[test]
    fn delivery_claim_rejects_negative_attempts() {
        assert!(
            StorageEventDeliveryClaim::try_new(
                EventDeliveryId::new(7).unwrap(),
                -1,
                Uuid::new_v4(),
            )
            .is_err()
        );
    }

    #[test]
    fn delivery_transport_values_reject_invalid_shapes() {
        assert!(
            StorageEventDeliverySink::try_new(
                EventSinkId::new(8).unwrap(),
                "sink",
                "webhook",
                serde_json::json!([]),
                None,
            )
            .is_err()
        );
        assert!(
            StorageEventDeliverySubscription::try_new(
                EventSubscriptionId::new(9).unwrap(),
                hubuum_domain::CollectionId::new(1).unwrap(),
                "subscription",
                serde_json::json!([]),
            )
            .is_err()
        );
    }

    #[test]
    fn retained_event_debug_output_redacts_the_serialized_payload() {
        let event = StorageRetainedEvent::try_new(
            EventSequence::new(11).unwrap(),
            r#"{"metadata":"payload-secret"}"#,
        )
        .unwrap();

        let debug = format!("{event:?}");

        assert!(debug.contains("11"));
        assert!(!debug.contains("payload-secret"));
    }

    #[test]
    fn retained_events_reject_non_object_json() {
        let error =
            StorageRetainedEvent::try_new(EventSequence::new(11).unwrap(), "[]").unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn retention_summary_reports_whether_the_backend_did_work() {
        assert!(!StorageEventRetentionSummary::default().did_work());
        assert!(StorageEventRetentionSummary::new(1, 0).did_work());
        assert!(StorageEventRetentionSummary::new(0, 1).did_work());
    }
}
