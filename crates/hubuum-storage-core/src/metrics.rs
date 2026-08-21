use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::ExportTemplateId;

use crate::{
    EventFanoutSnapshot, EventQueueSnapshot, StorageError, StorageTaskKind, StorageTaskStatus,
};

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
    oldest_queued_at: Option<DateTime<Utc>>,
    oldest_active_at: Option<DateTime<Utc>>,
}

impl TaskGaugeAge {
    #[must_use]
    pub const fn new(
        kind: StorageTaskKind,
        oldest_queued_at: Option<DateTime<Utc>>,
        oldest_active_at: Option<DateTime<Utc>>,
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
    pub const fn oldest_queued_at(self) -> Option<DateTime<Utc>> {
        self.oldest_queued_at
    }

    #[must_use]
    pub const fn oldest_active_at(self) -> Option<DateTime<Utc>> {
        self.oldest_active_at
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskGaugeLastTerminal {
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    finished_at: Option<DateTime<Utc>>,
}

impl TaskGaugeLastTerminal {
    #[must_use]
    pub const fn new(
        kind: StorageTaskKind,
        status: StorageTaskStatus,
        finished_at: Option<DateTime<Utc>>,
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
    pub const fn finished_at(self) -> Option<DateTime<Utc>> {
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
    async fn get_inventory_metrics_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError>;

    async fn get_task_metrics_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError>;

    async fn get_event_metrics_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError>;
}
