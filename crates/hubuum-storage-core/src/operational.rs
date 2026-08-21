use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    CollectionId, EventSinkId, EventSubscriptionId, ExportTemplateId, MaintenanceState,
    TokenRetentionSettings,
};
use std::fmt;

use crate::StorageError;

/// Backend-neutral readiness data used by probes and orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    storage_ready: bool,
    maintenance_state: MaintenanceState,
}

impl ReadinessSnapshot {
    #[must_use]
    pub const fn new(storage_ready: bool, maintenance_state: MaintenanceState) -> Self {
        Self {
            storage_ready,
            maintenance_state,
        }
    }

    #[must_use]
    pub const fn storage_is_ready(self) -> bool {
        self.storage_ready
    }

    #[must_use]
    pub const fn maintenance_state(self) -> MaintenanceState {
        self.maintenance_state
    }
}

/// Persisted task counts grouped by lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalTaskStatusCounts {
    total: i64,
    active: OperationalTaskActiveCounts,
    terminal: OperationalTaskTerminalCounts,
}

impl OperationalTaskStatusCounts {
    #[must_use]
    pub const fn new(
        total: i64,
        active: OperationalTaskActiveCounts,
        terminal: OperationalTaskTerminalCounts,
    ) -> Self {
        Self {
            total,
            active,
            terminal,
        }
    }

    #[must_use]
    pub const fn total(self) -> i64 {
        self.total
    }

    #[must_use]
    pub const fn queued(self) -> i64 {
        self.active.queued
    }

    #[must_use]
    pub const fn validating(self) -> i64 {
        self.active.validating
    }

    #[must_use]
    pub const fn running(self) -> i64 {
        self.active.running
    }

    #[must_use]
    pub const fn succeeded(self) -> i64 {
        self.terminal.succeeded
    }

    #[must_use]
    pub const fn failed(self) -> i64 {
        self.terminal.failed
    }

    #[must_use]
    pub const fn partially_succeeded(self) -> i64 {
        self.terminal.partially_succeeded
    }

    #[must_use]
    pub const fn cancelled(self) -> i64 {
        self.terminal.cancelled
    }
}

/// Persisted non-terminal task counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalTaskActiveCounts {
    queued: i64,
    validating: i64,
    running: i64,
}

impl OperationalTaskActiveCounts {
    #[must_use]
    pub const fn new(queued: i64, validating: i64, running: i64) -> Self {
        Self {
            queued,
            validating,
            running,
        }
    }
}

/// Persisted terminal task counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalTaskTerminalCounts {
    succeeded: i64,
    failed: i64,
    partially_succeeded: i64,
    cancelled: i64,
}

impl OperationalTaskTerminalCounts {
    #[must_use]
    pub const fn new(
        succeeded: i64,
        failed: i64,
        partially_succeeded: i64,
        cancelled: i64,
    ) -> Self {
        Self {
            succeeded,
            failed,
            partially_succeeded,
            cancelled,
        }
    }
}

/// Persisted task counts grouped by workflow kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalTaskKindCounts {
    imports: i64,
    exports: i64,
    reindexes: i64,
}

impl OperationalTaskKindCounts {
    #[must_use]
    pub const fn new(imports: i64, exports: i64, reindexes: i64) -> Self {
        Self {
            imports,
            exports,
            reindexes,
        }
    }

    #[must_use]
    pub const fn imports(self) -> i64 {
        self.imports
    }

    #[must_use]
    pub const fn exports(self) -> i64 {
        self.exports
    }

    #[must_use]
    pub const fn reindexes(self) -> i64 {
        self.reindexes
    }
}

/// Backend-neutral task queue state for administrator diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationalTaskQueueSnapshot {
    statuses: OperationalTaskStatusCounts,
    kinds: OperationalTaskKindCounts,
    total_task_events: i64,
    total_import_result_rows: i64,
    oldest_queued_at: Option<DateTime<Utc>>,
    oldest_active_at: Option<DateTime<Utc>>,
}

/// Aggregated execution health for one export-template identity.
#[derive(Debug, Clone, PartialEq)]
pub struct OperationalExportTemplateHealth {
    template_name: Option<String>,
    runs: i64,
    warning_total: i64,
    warning_max: i32,
    total_duration_ms_total: i64,
    total_duration_ms_max: i32,
}

/// Stored template material required by the explicit administrator audit.
#[derive(Clone, PartialEq, Eq)]
pub struct OperationalExportTemplateAuditEntry {
    id: ExportTemplateId,
    collection_id: CollectionId,
    name: String,
    template: String,
    content_type: String,
}

impl OperationalExportTemplateAuditEntry {
    #[must_use]
    pub fn new(
        id: ExportTemplateId,
        collection_id: CollectionId,
        name: impl Into<String>,
        template: impl Into<String>,
        content_type: impl Into<String>,
    ) -> Self {
        Self {
            id,
            collection_id,
            name: name.into(),
            template: template.into(),
            content_type: content_type.into(),
        }
    }

    #[must_use]
    pub const fn id(&self) -> ExportTemplateId {
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
    pub fn template(&self) -> &str {
        &self.template
    }

    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
}

impl fmt::Debug for OperationalExportTemplateAuditEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OperationalExportTemplateAuditEntry")
            .field("id", &self.id)
            .field("collection_id", &self.collection_id)
            .field("name", &"<redacted>")
            .field("template", &"<redacted>")
            .field("content_type", &self.content_type)
            .finish()
    }
}

impl OperationalExportTemplateHealth {
    #[must_use]
    pub fn new(
        template_name: Option<String>,
        runs: i64,
        warning_total: i64,
        warning_max: i32,
        total_duration_ms_total: i64,
        total_duration_ms_max: i32,
    ) -> Self {
        Self {
            template_name,
            runs,
            warning_total,
            warning_max,
            total_duration_ms_total,
            total_duration_ms_max,
        }
    }

    #[must_use]
    pub fn template_name(&self) -> Option<&str> {
        self.template_name.as_deref()
    }

    #[must_use]
    pub const fn runs(&self) -> i64 {
        self.runs
    }

    #[must_use]
    pub const fn warning_total(&self) -> i64 {
        self.warning_total
    }

    #[must_use]
    pub const fn warning_max(&self) -> i32 {
        self.warning_max
    }

    #[must_use]
    pub const fn total_duration_ms_total(&self) -> i64 {
        self.total_duration_ms_total
    }

    #[must_use]
    pub const fn total_duration_ms_max(&self) -> i32 {
        self.total_duration_ms_max
    }
}

impl OperationalTaskQueueSnapshot {
    #[must_use]
    pub const fn new(
        statuses: OperationalTaskStatusCounts,
        kinds: OperationalTaskKindCounts,
        total_task_events: i64,
        total_import_result_rows: i64,
        oldest_queued_at: Option<DateTime<Utc>>,
        oldest_active_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            statuses,
            kinds,
            total_task_events,
            total_import_result_rows,
            oldest_queued_at,
            oldest_active_at,
        }
    }

    #[must_use]
    pub const fn statuses(self) -> OperationalTaskStatusCounts {
        self.statuses
    }

    #[must_use]
    pub const fn kinds(self) -> OperationalTaskKindCounts {
        self.kinds
    }

    #[must_use]
    pub const fn total_task_events(self) -> i64 {
        self.total_task_events
    }

    #[must_use]
    pub const fn total_import_result_rows(self) -> i64 {
        self.total_import_result_rows
    }

    #[must_use]
    pub const fn oldest_queued_at(self) -> Option<DateTime<Utc>> {
        self.oldest_queued_at
    }

    #[must_use]
    pub const fn oldest_active_at(self) -> Option<DateTime<Utc>> {
        self.oldest_active_at
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
    id: EventSinkId,
    name: String,
    kind: String,
    enabled: bool,
}

impl EventSinkSnapshot {
    #[must_use]
    pub fn new(id: EventSinkId, name: String, kind: String, enabled: bool) -> Self {
        Self {
            id,
            name,
            kind,
            enabled,
        }
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
    id: EventSubscriptionId,
    name: String,
    collection_id: CollectionId,
    enabled: bool,
    sink: EventSinkSnapshot,
    queue: EventQueueSnapshot,
}

impl EventSubscriptionHealthSnapshot {
    #[must_use]
    pub fn new(
        id: EventSubscriptionId,
        name: String,
        collection_id: CollectionId,
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
    pub const fn id(&self) -> EventSubscriptionId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
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
    async fn get_readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError>;

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError>;

    async fn get_task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError>;

    /// Return one backend-aggregated row per stored export-template identity.
    /// Implementations must not return individual export outputs.
    async fn get_export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError>;

    /// Load the complete stored template set required for an explicit
    /// administrator validation pass.
    async fn list_export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError>;
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
    async fn get_event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::OperationalExportTemplateAuditEntry;
    use hubuum_domain::{CollectionId, ExportTemplateId};

    #[test]
    fn export_template_audit_debug_redacts_name_and_source() {
        let entry = OperationalExportTemplateAuditEntry::new(
            ExportTemplateId::new(1).unwrap(),
            CollectionId::new(2).unwrap(),
            "sensitive-template-name",
            "sensitive-template-source",
            "text/plain",
        );
        let debug = format!("{entry:?}");

        assert!(!debug.contains("sensitive-template-name"));
        assert!(!debug.contains("sensitive-template-source"));
        assert!(debug.contains("text/plain"));
    }
}
