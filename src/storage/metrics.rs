use async_trait::async_trait;
use chrono::NaiveDateTime;

use crate::models::{ExportTemplateID, TaskKind, TaskStatus};

use super::{EventFanoutSnapshot, EventQueueSnapshot, StorageError};

/// Backend-neutral state for the configured storage connection pool.
#[derive(Debug)]
pub(crate) struct StoragePoolState {
    pub(crate) max_connections: u32,
    pub(crate) total_connections: u32,
    pub(crate) available_connections: u32,
    pub(crate) idle_connections: u32,
    pub(crate) in_use_connections: u32,
    pub(crate) pending_acquisitions: u64,
    pub(crate) acquisitions_started: u64,
    pub(crate) acquisitions_direct: u64,
    pub(crate) acquisitions_waited: u64,
    pub(crate) acquisitions_timed_out: u64,
    pub(crate) acquisition_wait_time_ms: u64,
    pub(crate) connections_created: u64,
    pub(crate) connections_closed_broken: u64,
    pub(crate) connections_closed_invalid: u64,
    pub(crate) connections_closed_max_lifetime: u64,
    pub(crate) connections_closed_idle_timeout: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct InventoryMetricsSnapshot {
    pub(crate) collections: i64,
    pub(crate) classes: i64,
    pub(crate) objects: i64,
    pub(crate) users: i64,
    pub(crate) groups: i64,
    pub(crate) service_accounts: i64,
    pub(crate) remote_targets: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct ExportTemplateMetricIdentity {
    pub(crate) id: ExportTemplateID,
    pub(crate) name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct InventoryGaugeSnapshot {
    pub(crate) counts: InventoryMetricsSnapshot,
    pub(crate) export_templates: Vec<ExportTemplateMetricIdentity>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskGaugeCount {
    pub(crate) kind: TaskKind,
    pub(crate) status: TaskStatus,
    pub(crate) count: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskGaugeAge {
    pub(crate) kind: TaskKind,
    pub(crate) oldest_queued_at: Option<NaiveDateTime>,
    pub(crate) oldest_active_at: Option<NaiveDateTime>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskGaugeLastTerminal {
    pub(crate) kind: TaskKind,
    pub(crate) status: TaskStatus,
    pub(crate) finished_at: Option<NaiveDateTime>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskGaugeSnapshot {
    pub(crate) counts: Vec<TaskGaugeCount>,
    pub(crate) ages: Vec<TaskGaugeAge>,
    pub(crate) last_terminal: Vec<TaskGaugeLastTerminal>,
}

#[derive(Debug, Clone)]
pub(crate) struct EventMetricsSnapshot {
    pub(crate) fanout: EventFanoutSnapshot,
    pub(crate) delivery: EventQueueSnapshot,
}

/// Metrics data every selectable storage backend must provide.
///
/// Implementations translate their native failures into [`StorageError`]
/// before crossing this boundary. Application metrics code therefore neither
/// selects a database nor knows how its queries and pool are implemented.
#[async_trait]
pub(crate) trait MetricsStorage: Send + Sync {
    fn metrics_pool_state(&self) -> StoragePoolState;

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError>;

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError>;

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError>;
}
