use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_domain::ExportTemplateId;

use crate::{
    EventFanoutSnapshot, EventQueueSnapshot, StorageError, StorageTaskKind, StorageTaskStatus,
};

/// Capacity and current occupancy of a backend-owned connection pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoragePoolCapacity {
    max_connections: u32,
    total_connections: u32,
    available_connections: u32,
    idle_connections: u32,
    in_use_connections: u32,
}

impl StoragePoolCapacity {
    #[must_use]
    pub const fn new(
        max_connections: u32,
        total_connections: u32,
        available_connections: u32,
        idle_connections: u32,
        in_use_connections: u32,
    ) -> Self {
        Self {
            max_connections,
            total_connections,
            available_connections,
            idle_connections,
            in_use_connections,
        }
    }

    #[must_use]
    pub const fn max_connections(self) -> u32 {
        self.max_connections
    }

    #[must_use]
    pub const fn total_connections(self) -> u32 {
        self.total_connections
    }

    #[must_use]
    pub const fn available_connections(self) -> u32 {
        self.available_connections
    }

    #[must_use]
    pub const fn idle_connections(self) -> u32 {
        self.idle_connections
    }

    #[must_use]
    pub const fn in_use_connections(self) -> u32 {
        self.in_use_connections
    }
}

/// Acquisition counters reported by a backend-owned connection pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoragePoolAcquisitionState {
    pending: u64,
    started: u64,
    direct: u64,
    waited: u64,
    timed_out: u64,
    wait_time_ms: u64,
}

impl StoragePoolAcquisitionState {
    #[must_use]
    pub const fn new(
        pending: u64,
        started: u64,
        direct: u64,
        waited: u64,
        timed_out: u64,
        wait_time_ms: u64,
    ) -> Self {
        Self {
            pending,
            started,
            direct,
            waited,
            timed_out,
            wait_time_ms,
        }
    }

    #[must_use]
    pub const fn pending(self) -> u64 {
        self.pending
    }

    #[must_use]
    pub const fn started(self) -> u64 {
        self.started
    }

    #[must_use]
    pub const fn direct(self) -> u64 {
        self.direct
    }

    #[must_use]
    pub const fn waited(self) -> u64 {
        self.waited
    }

    #[must_use]
    pub const fn timed_out(self) -> u64 {
        self.timed_out
    }

    #[must_use]
    pub const fn wait_time_ms(self) -> u64 {
        self.wait_time_ms
    }
}

/// Connection lifecycle counters reported by a backend-owned pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoragePoolConnectionState {
    created: u64,
    closed_broken: u64,
    closed_invalid: u64,
    closed_max_lifetime: u64,
    closed_idle_timeout: u64,
}

impl StoragePoolConnectionState {
    #[must_use]
    pub const fn new(
        created: u64,
        closed_broken: u64,
        closed_invalid: u64,
        closed_max_lifetime: u64,
        closed_idle_timeout: u64,
    ) -> Self {
        Self {
            created,
            closed_broken,
            closed_invalid,
            closed_max_lifetime,
            closed_idle_timeout,
        }
    }

    #[must_use]
    pub const fn created(self) -> u64 {
        self.created
    }

    #[must_use]
    pub const fn closed_broken(self) -> u64 {
        self.closed_broken
    }

    #[must_use]
    pub const fn closed_invalid(self) -> u64 {
        self.closed_invalid
    }

    #[must_use]
    pub const fn closed_max_lifetime(self) -> u64 {
        self.closed_max_lifetime
    }

    #[must_use]
    pub const fn closed_idle_timeout(self) -> u64 {
        self.closed_idle_timeout
    }
}

/// Backend-neutral state for the configured storage connection pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoragePoolState {
    capacity: StoragePoolCapacity,
    acquisitions: StoragePoolAcquisitionState,
    connections: StoragePoolConnectionState,
}

impl StoragePoolState {
    #[must_use]
    pub const fn new(
        capacity: StoragePoolCapacity,
        acquisitions: StoragePoolAcquisitionState,
        connections: StoragePoolConnectionState,
    ) -> Self {
        Self {
            capacity,
            acquisitions,
            connections,
        }
    }

    #[must_use]
    pub const fn capacity(self) -> StoragePoolCapacity {
        self.capacity
    }

    #[must_use]
    pub const fn acquisitions(self) -> StoragePoolAcquisitionState {
        self.acquisitions
    }

    #[must_use]
    pub const fn connections(self) -> StoragePoolConnectionState {
        self.connections
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryMetricsSnapshot {
    collections: i64,
    classes: i64,
    objects: i64,
    users: i64,
    groups: i64,
    service_accounts: i64,
    remote_targets: i64,
}

impl InventoryMetricsSnapshot {
    #[must_use]
    pub const fn new(
        collections: i64,
        classes: i64,
        objects: i64,
        users: i64,
        groups: i64,
        service_accounts: i64,
        remote_targets: i64,
    ) -> Self {
        Self {
            collections,
            classes,
            objects,
            users,
            groups,
            service_accounts,
            remote_targets,
        }
    }

    #[must_use]
    pub const fn collections(self) -> i64 {
        self.collections
    }

    #[must_use]
    pub const fn classes(self) -> i64 {
        self.classes
    }

    #[must_use]
    pub const fn objects(self) -> i64 {
        self.objects
    }

    #[must_use]
    pub const fn users(self) -> i64 {
        self.users
    }

    #[must_use]
    pub const fn groups(self) -> i64 {
        self.groups
    }

    #[must_use]
    pub const fn service_accounts(self) -> i64 {
        self.service_accounts
    }

    #[must_use]
    pub const fn remote_targets(self) -> i64 {
        self.remote_targets
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportTemplateMetricIdentity {
    id: ExportTemplateId,
    name: String,
}

impl ExportTemplateMetricIdentity {
    #[must_use]
    pub fn new(id: ExportTemplateId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }

    #[must_use]
    pub const fn id(&self) -> ExportTemplateId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::ExportTemplateMetricIdentity;

    #[test]
    fn export_template_metric_identity_requires_a_positive_id() {
        let identity = ExportTemplateMetricIdentity::new(
            hubuum_domain::ExportTemplateId::new(1).unwrap(),
            "valid",
        );
        assert_eq!(identity.id().id(), 1);
        assert_eq!(identity.name(), "valid");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InventoryGaugeSnapshot {
    counts: InventoryMetricsSnapshot,
    export_templates: Vec<ExportTemplateMetricIdentity>,
}

impl InventoryGaugeSnapshot {
    #[must_use]
    pub fn new(
        counts: InventoryMetricsSnapshot,
        export_templates: Vec<ExportTemplateMetricIdentity>,
    ) -> Self {
        Self {
            counts,
            export_templates,
        }
    }

    #[must_use]
    pub const fn counts(&self) -> InventoryMetricsSnapshot {
        self.counts
    }

    #[must_use]
    pub fn export_templates(&self) -> &[ExportTemplateMetricIdentity] {
        &self.export_templates
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskGaugeCount {
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    count: i64,
}

impl TaskGaugeCount {
    #[must_use]
    pub const fn new(kind: StorageTaskKind, status: StorageTaskStatus, count: i64) -> Self {
        Self {
            kind,
            status,
            count,
        }
    }

    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn status(self) -> StorageTaskStatus {
        self.status
    }

    #[must_use]
    pub const fn count(self) -> i64 {
        self.count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskGaugeAge {
    kind: StorageTaskKind,
    oldest_queued_at: Option<NaiveDateTime>,
    oldest_active_at: Option<NaiveDateTime>,
}

impl TaskGaugeAge {
    #[must_use]
    pub const fn new(
        kind: StorageTaskKind,
        oldest_queued_at: Option<NaiveDateTime>,
        oldest_active_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            kind,
            oldest_queued_at,
            oldest_active_at,
        }
    }

    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn oldest_queued_at(self) -> Option<NaiveDateTime> {
        self.oldest_queued_at
    }

    #[must_use]
    pub const fn oldest_active_at(self) -> Option<NaiveDateTime> {
        self.oldest_active_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskGaugeLastTerminal {
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    finished_at: Option<NaiveDateTime>,
}

impl TaskGaugeLastTerminal {
    #[must_use]
    pub const fn new(
        kind: StorageTaskKind,
        status: StorageTaskStatus,
        finished_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            kind,
            status,
            finished_at,
        }
    }

    #[must_use]
    pub const fn kind(self) -> StorageTaskKind {
        self.kind
    }

    #[must_use]
    pub const fn status(self) -> StorageTaskStatus {
        self.status
    }

    #[must_use]
    pub const fn finished_at(self) -> Option<NaiveDateTime> {
        self.finished_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskGaugeSnapshot {
    counts: Vec<TaskGaugeCount>,
    ages: Vec<TaskGaugeAge>,
    last_terminal: Vec<TaskGaugeLastTerminal>,
}

impl TaskGaugeSnapshot {
    #[must_use]
    pub fn new(
        counts: Vec<TaskGaugeCount>,
        ages: Vec<TaskGaugeAge>,
        last_terminal: Vec<TaskGaugeLastTerminal>,
    ) -> Self {
        Self {
            counts,
            ages,
            last_terminal,
        }
    }

    #[must_use]
    pub fn counts(&self) -> &[TaskGaugeCount] {
        &self.counts
    }

    #[must_use]
    pub fn ages(&self) -> &[TaskGaugeAge] {
        &self.ages
    }

    #[must_use]
    pub fn last_terminal(&self) -> &[TaskGaugeLastTerminal] {
        &self.last_terminal
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventMetricsSnapshot {
    fanout: EventFanoutSnapshot,
    delivery: EventQueueSnapshot,
}

impl EventMetricsSnapshot {
    #[must_use]
    pub const fn new(fanout: EventFanoutSnapshot, delivery: EventQueueSnapshot) -> Self {
        Self { fanout, delivery }
    }

    #[must_use]
    pub const fn fanout(self) -> EventFanoutSnapshot {
        self.fanout
    }

    #[must_use]
    pub const fn delivery(self) -> EventQueueSnapshot {
        self.delivery
    }
}

/// Metrics data every selectable storage backend must provide.
///
/// Implementations translate their native failures into [`StorageError`]
/// before crossing this boundary. Application metrics code therefore neither
/// selects a database nor knows how its queries and pool are implemented.
#[async_trait]
pub trait MetricsStorage: Send + Sync {
    fn metrics_pool_state(&self) -> StoragePoolState;

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError>;

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError>;

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError>;
}
