use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    CollectionId, EventSinkId, EventSubscriptionId, ExportTemplateId, MaintenanceState,
    TokenRetentionSettings,
};
use std::fmt;

use crate::{StorageError, StorageValidationError};

fn checked_nonnegative_sum(
    values: &[i64],
    description: &'static str,
) -> Result<i64, StorageValidationError> {
    if values.iter().any(|value| *value < 0) {
        return Err(StorageValidationError::invalid(format!(
            "{description} must not be negative"
        )));
    }
    values.iter().try_fold(0_i64, |sum, value| {
        sum.checked_add(*value).ok_or_else(|| {
            StorageValidationError::invalid(format!("{description} must not overflow"))
        })
    })
}

/// Backend-neutral readiness data used by probes and orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageReadinessSnapshot {
    storage_ready: bool,
    maintenance_state: MaintenanceState,
}

impl StorageReadinessSnapshot {
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
pub struct StorageOperationalTaskStatusCounts {
    total: i64,
    active: StorageOperationalTaskActiveCounts,
    terminal: StorageOperationalTaskTerminalCounts,
}

impl StorageOperationalTaskStatusCounts {
    pub fn try_new(
        total: i64,
        active: StorageOperationalTaskActiveCounts,
        terminal: StorageOperationalTaskTerminalCounts,
    ) -> Result<Self, StorageValidationError> {
        let status_total = checked_nonnegative_sum(
            &[
                active.queued,
                active.validating,
                active.running,
                terminal.succeeded,
                terminal.failed,
                terminal.partially_succeeded,
                terminal.cancelled,
            ],
            "task status counts",
        )?;
        if total < 0 || status_total != total {
            return Err(StorageValidationError::invalid(
                "task status counts must be nonnegative and sum to total",
            ));
        }
        Ok(Self {
            total,
            active,
            terminal,
        })
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
pub struct StorageOperationalTaskActiveCounts {
    queued: i64,
    validating: i64,
    running: i64,
}

impl StorageOperationalTaskActiveCounts {
    pub fn try_new(
        queued: i64,
        validating: i64,
        running: i64,
    ) -> Result<Self, StorageValidationError> {
        checked_nonnegative_sum(&[queued, validating, running], "active task counts")?;
        Ok(Self {
            queued,
            validating,
            running,
        })
    }
}

/// Persisted terminal task counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageOperationalTaskTerminalCounts {
    succeeded: i64,
    failed: i64,
    partially_succeeded: i64,
    cancelled: i64,
}

impl StorageOperationalTaskTerminalCounts {
    pub fn try_new(
        succeeded: i64,
        failed: i64,
        partially_succeeded: i64,
        cancelled: i64,
    ) -> Result<Self, StorageValidationError> {
        checked_nonnegative_sum(
            &[succeeded, failed, partially_succeeded, cancelled],
            "terminal task counts",
        )?;
        Ok(Self {
            succeeded,
            failed,
            partially_succeeded,
            cancelled,
        })
    }
}

/// Persisted task counts grouped by workflow kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageOperationalTaskKindCounts {
    imports: i64,
    exports: i64,
    reindexes: i64,
}

impl StorageOperationalTaskKindCounts {
    pub fn try_new(
        imports: i64,
        exports: i64,
        reindexes: i64,
    ) -> Result<Self, StorageValidationError> {
        checked_nonnegative_sum(&[imports, exports, reindexes], "task kind counts")?;
        Ok(Self {
            imports,
            exports,
            reindexes,
        })
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
pub struct StorageOperationalTaskQueueSnapshot {
    statuses: StorageOperationalTaskStatusCounts,
    kinds: StorageOperationalTaskKindCounts,
    total_task_events: i64,
    total_import_result_rows: i64,
    oldest_queued_at: Option<DateTime<Utc>>,
    oldest_active_at: Option<DateTime<Utc>>,
}

/// Aggregated execution health for one export-template identity.
#[derive(Debug, Clone, PartialEq)]
pub struct StorageOperationalExportTemplateHealth {
    template_name: Option<String>,
    runs: i64,
    warning_total: i64,
    warning_max: i32,
    total_duration_ms_total: i64,
    total_duration_ms_max: i32,
}

/// Stored template material required by the explicit administrator audit.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageOperationalExportTemplateAuditEntry {
    id: ExportTemplateId,
    collection_id: CollectionId,
    name: String,
    template: String,
    content_type: String,
}

impl StorageOperationalExportTemplateAuditEntry {
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

impl fmt::Debug for StorageOperationalExportTemplateAuditEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageOperationalExportTemplateAuditEntry")
            .field("id", &self.id)
            .field("collection_id", &self.collection_id)
            .field("name", &"<redacted>")
            .field("template", &"<redacted>")
            .field("content_type", &self.content_type)
            .finish()
    }
}

impl StorageOperationalExportTemplateHealth {
    pub fn try_new(
        template_name: Option<String>,
        runs: i64,
        warning_total: i64,
        warning_max: i32,
        total_duration_ms_total: i64,
        total_duration_ms_max: i32,
    ) -> Result<Self, StorageValidationError> {
        checked_nonnegative_sum(
            &[
                runs,
                warning_total,
                i64::from(warning_max),
                total_duration_ms_total,
                i64::from(total_duration_ms_max),
            ],
            "export-template health values",
        )?;
        if i64::from(warning_max) > warning_total
            || i64::from(total_duration_ms_max) > total_duration_ms_total
            || (runs == 0
                && (warning_total != 0
                    || warning_max != 0
                    || total_duration_ms_total != 0
                    || total_duration_ms_max != 0))
        {
            return Err(StorageValidationError::invalid(
                "export-template health maxima and totals are inconsistent",
            ));
        }
        Ok(Self {
            template_name,
            runs,
            warning_total,
            warning_max,
            total_duration_ms_total,
            total_duration_ms_max,
        })
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

impl StorageOperationalTaskQueueSnapshot {
    pub fn try_new(
        statuses: StorageOperationalTaskStatusCounts,
        kinds: StorageOperationalTaskKindCounts,
        total_task_events: i64,
        total_import_result_rows: i64,
        oldest_queued_at: Option<DateTime<Utc>>,
        oldest_active_at: Option<DateTime<Utc>>,
    ) -> Result<Self, StorageValidationError> {
        checked_nonnegative_sum(
            &[total_task_events, total_import_result_rows],
            "task queue aggregate counts",
        )?;
        let active_count = statuses
            .validating()
            .checked_add(statuses.running())
            .ok_or_else(|| {
                StorageValidationError::invalid("active task counts must not overflow")
            })?;
        if oldest_queued_at.is_some() != (statuses.queued() > 0)
            || oldest_active_at.is_some() != (active_count > 0)
        {
            return Err(StorageValidationError::invalid(
                "task queue counts and oldest timestamps are inconsistent",
            ));
        }
        Ok(Self {
            statuses,
            kinds,
            total_task_events,
            total_import_result_rows,
            oldest_queued_at,
            oldest_active_at,
        })
    }

    #[must_use]
    pub const fn statuses(self) -> StorageOperationalTaskStatusCounts {
        self.statuses
    }

    #[must_use]
    pub const fn kinds(self) -> StorageOperationalTaskKindCounts {
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
pub struct StorageEventDeliveryStatusSnapshot {
    total: i64,
    pending: i64,
    in_flight: i64,
    succeeded: i64,
    failed: i64,
    dead: i64,
    retryable: i64,
}

impl StorageEventDeliveryStatusSnapshot {
    pub fn try_new(
        total: i64,
        pending: i64,
        in_flight: i64,
        succeeded: i64,
        failed: i64,
        dead: i64,
        retryable: i64,
    ) -> Result<Self, StorageValidationError> {
        let status_total = checked_nonnegative_sum(
            &[pending, in_flight, succeeded, failed, dead],
            "event delivery status counts",
        )?;
        if total < 0 || retryable < 0 || status_total != total || retryable > failed {
            return Err(StorageValidationError::invalid(
                "event delivery status counts are inconsistent",
            ));
        }
        Ok(Self {
            total,
            pending,
            in_flight,
            succeeded,
            failed,
            dead,
            retryable,
        })
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
pub struct StorageEventFanoutSnapshot {
    pending_events: i64,
    in_flight_events: i64,
    stale_claims: i64,
    oldest_pending_age_seconds: Option<i64>,
}

impl StorageEventFanoutSnapshot {
    pub fn try_new(
        pending_events: i64,
        in_flight_events: i64,
        stale_claims: i64,
        oldest_pending_age_seconds: Option<i64>,
    ) -> Result<Self, StorageValidationError> {
        checked_nonnegative_sum(
            &[pending_events, in_flight_events, stale_claims],
            "event fan-out counts",
        )?;
        if in_flight_events
            .checked_add(stale_claims)
            .is_none_or(|value| value > pending_events)
            || oldest_pending_age_seconds.is_some_and(|value| value < 0)
            || oldest_pending_age_seconds.is_some() != (pending_events > 0)
        {
            return Err(StorageValidationError::invalid(
                "event fan-out counts and ages are inconsistent",
            ));
        }
        Ok(Self {
            pending_events,
            in_flight_events,
            stale_claims,
            oldest_pending_age_seconds,
        })
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
pub struct StorageEventQueueSnapshot {
    counts: StorageEventDeliveryStatusSnapshot,
    stale_claims: i64,
    oldest_due_age_seconds: Option<i64>,
}

impl StorageEventQueueSnapshot {
    pub fn try_new(
        counts: StorageEventDeliveryStatusSnapshot,
        stale_claims: i64,
        oldest_due_age_seconds: Option<i64>,
    ) -> Result<Self, StorageValidationError> {
        let due_events = counts
            .pending
            .checked_add(counts.retryable)
            .and_then(|value| value.checked_add(stale_claims))
            .ok_or_else(|| {
                StorageValidationError::invalid("event delivery due counts must not overflow")
            })?;
        if stale_claims < 0
            || stale_claims > counts.in_flight
            || oldest_due_age_seconds.is_some_and(|value| value < 0)
            || oldest_due_age_seconds.is_some() != (due_events > 0)
        {
            return Err(StorageValidationError::invalid(
                "event delivery queue counts and ages are inconsistent",
            ));
        }
        Ok(Self {
            counts,
            stale_claims,
            oldest_due_age_seconds,
        })
    }

    #[must_use]
    pub const fn counts(self) -> StorageEventDeliveryStatusSnapshot {
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
pub struct StorageEventSinkSnapshot {
    id: EventSinkId,
    name: String,
    kind: String,
    enabled: bool,
}

impl StorageEventSinkSnapshot {
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
pub struct StorageEventSinkHealthSnapshot {
    sink: StorageEventSinkSnapshot,
    subscription_count: i64,
    queue: StorageEventQueueSnapshot,
}

impl StorageEventSinkHealthSnapshot {
    pub fn try_new(
        sink: StorageEventSinkSnapshot,
        subscription_count: i64,
        queue: StorageEventQueueSnapshot,
    ) -> Result<Self, StorageValidationError> {
        if subscription_count < 0 {
            return Err(StorageValidationError::invalid(
                "event sink subscription count must not be negative",
            ));
        }
        Ok(Self {
            sink,
            subscription_count,
            queue,
        })
    }

    #[must_use]
    pub const fn sink(&self) -> &StorageEventSinkSnapshot {
        &self.sink
    }

    #[must_use]
    pub const fn subscription_count(&self) -> i64 {
        self.subscription_count
    }

    #[must_use]
    pub const fn queue(&self) -> StorageEventQueueSnapshot {
        self.queue
    }
}

/// Persisted queue health grouped by subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageEventSubscriptionHealthSnapshot {
    id: EventSubscriptionId,
    name: String,
    collection_id: CollectionId,
    enabled: bool,
    sink: StorageEventSinkSnapshot,
    queue: StorageEventQueueSnapshot,
}

impl StorageEventSubscriptionHealthSnapshot {
    #[must_use]
    pub fn new(
        id: EventSubscriptionId,
        name: String,
        collection_id: CollectionId,
        enabled: bool,
        sink: StorageEventSinkSnapshot,
        queue: StorageEventQueueSnapshot,
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
    pub const fn sink(&self) -> &StorageEventSinkSnapshot {
        &self.sink
    }

    #[must_use]
    pub const fn queue(&self) -> StorageEventQueueSnapshot {
        self.queue
    }
}

/// Complete persisted event-pipeline health snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageEventDeliveryHealthSnapshot {
    fanout: StorageEventFanoutSnapshot,
    delivery: StorageEventQueueSnapshot,
    sinks: Vec<StorageEventSinkHealthSnapshot>,
    subscriptions: Vec<StorageEventSubscriptionHealthSnapshot>,
}

impl StorageEventDeliveryHealthSnapshot {
    #[must_use]
    pub const fn new(
        fanout: StorageEventFanoutSnapshot,
        delivery: StorageEventQueueSnapshot,
        sinks: Vec<StorageEventSinkHealthSnapshot>,
        subscriptions: Vec<StorageEventSubscriptionHealthSnapshot>,
    ) -> Self {
        Self {
            fanout,
            delivery,
            sinks,
            subscriptions,
        }
    }

    #[must_use]
    pub const fn fanout(&self) -> StorageEventFanoutSnapshot {
        self.fanout
    }

    #[must_use]
    pub const fn delivery(&self) -> StorageEventQueueSnapshot {
        self.delivery
    }

    #[must_use]
    pub fn sinks(&self) -> &[StorageEventSinkHealthSnapshot] {
        &self.sinks
    }

    #[must_use]
    pub fn subscriptions(&self) -> &[StorageEventSubscriptionHealthSnapshot] {
        &self.subscriptions
    }
}

/// Operational state every selectable storage backend must expose.
#[async_trait]
pub trait OperationalStateStorage: Send + Sync {
    async fn get_readiness_snapshot(&self) -> Result<StorageReadinessSnapshot, StorageError>;

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError>;

    async fn get_task_queue_snapshot(
        &self,
    ) -> Result<StorageOperationalTaskQueueSnapshot, StorageError>;

    /// Return one backend-aggregated row per stored export-template identity.
    /// Implementations must not return individual export outputs.
    async fn load_export_template_health(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateHealth>, StorageError>;

    /// Load the complete stored template set required for an explicit
    /// administrator validation pass.
    async fn load_export_templates_for_audit(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateAuditEntry>, StorageError>;
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
    async fn get_event_delivery_health(
        &self,
    ) -> Result<StorageEventDeliveryHealthSnapshot, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_domain::{CollectionId, ExportTemplateId};

    fn task_statuses(
        queued: i64,
        validating: i64,
        running: i64,
    ) -> StorageOperationalTaskStatusCounts {
        let active =
            StorageOperationalTaskActiveCounts::try_new(queued, validating, running).unwrap();
        let terminal = StorageOperationalTaskTerminalCounts::try_new(0, 0, 0, 0).unwrap();
        StorageOperationalTaskStatusCounts::try_new(queued + validating + running, active, terminal)
            .unwrap()
    }

    #[test]
    fn export_template_audit_debug_redacts_name_and_source() {
        let entry = StorageOperationalExportTemplateAuditEntry::new(
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

    #[test]
    fn task_queue_snapshot_requires_an_oldest_queued_timestamp_for_queued_tasks() {
        let error = StorageOperationalTaskQueueSnapshot::try_new(
            task_statuses(1, 0, 0),
            StorageOperationalTaskKindCounts::try_new(1, 0, 0).unwrap(),
            0,
            0,
            None,
            None,
        )
        .unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn event_fanout_snapshot_requires_an_age_for_pending_events() {
        let error = StorageEventFanoutSnapshot::try_new(1, 0, 0, None).unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }

    #[test]
    fn event_queue_snapshot_requires_an_age_for_due_deliveries() {
        let counts = StorageEventDeliveryStatusSnapshot::try_new(1, 1, 0, 0, 0, 0, 0).unwrap();
        let error = StorageEventQueueSnapshot::try_new(counts, 0, None).unwrap_err();

        assert_eq!(
            error.kind(),
            crate::StorageValidationErrorKind::InvalidValue
        );
    }
}
