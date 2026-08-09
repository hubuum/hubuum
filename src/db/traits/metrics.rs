use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::dsl::{count_star, max, min};
use diesel::sql_types::BigInt;

use crate::db::prelude::*;
use crate::db::{DbConnection, with_connection};
use crate::errors::ApiError;
use crate::models::{
    EventDeliveryQueueHealth, EventFanoutHealth, ExportTemplateID, TaskKind, TaskStatus,
};
use crate::schema::{export_templates, tasks};
use crate::traits::BackendContext;

#[derive(Debug, Clone, Copy, QueryableByName)]
pub struct InventoryMetricsSnapshot {
    #[diesel(sql_type = BigInt)]
    pub collections: i64,
    #[diesel(sql_type = BigInt)]
    pub classes: i64,
    #[diesel(sql_type = BigInt)]
    pub objects: i64,
    #[diesel(sql_type = BigInt)]
    pub users: i64,
    #[diesel(sql_type = BigInt)]
    pub groups: i64,
    #[diesel(sql_type = BigInt)]
    pub service_accounts: i64,
    #[diesel(sql_type = BigInt)]
    pub remote_targets: i64,
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
pub struct TaskMetricsCount {
    pub kind: String,
    pub status: String,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct TaskMetricsSnapshot {
    pub counts: Vec<TaskMetricsCount>,
    pub oldest_queued_at: Option<NaiveDateTime>,
    pub oldest_active_at: Option<NaiveDateTime>,
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
pub struct EventMetricsSnapshot {
    pub fanout: EventFanoutHealth,
    pub delivery: EventDeliveryQueueHealth,
}

pub trait MetricsBackend {
    async fn metrics_inventory_snapshot(&self) -> Result<InventoryMetricsSnapshot, ApiError>;
    async fn metrics_task_snapshot(&self) -> Result<TaskMetricsSnapshot, ApiError>;
}

pub(crate) trait MetricsRefreshBackend {
    async fn metrics_inventory_gauge_snapshot(&self) -> Result<InventoryGaugeSnapshot, ApiError>;
    async fn metrics_task_gauge_snapshot(&self) -> Result<TaskGaugeSnapshot, ApiError>;
}

impl<T> MetricsBackend for T
where
    T: BackendContext + Sync + ?Sized,
{
    async fn metrics_inventory_snapshot(&self) -> Result<InventoryMetricsSnapshot, ApiError> {
        with_connection(crate::traits::backend_pool(self), load_inventory_counts).await
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskMetricsSnapshot, ApiError> {
        with_connection(crate::traits::backend_pool(self), async |conn| {
            let counts = load_task_count_rows(conn)
                .await?
                .into_iter()
                .map(|(kind, status, count)| TaskMetricsCount {
                    kind,
                    status,
                    count,
                })
                .collect();
            let oldest_queued_at = tasks::table
                .filter(tasks::status.eq(TaskStatus::Queued.as_str()))
                .select(min(tasks::created_at))
                .get_result::<Option<NaiveDateTime>>(conn)
                .await?;
            let oldest_active_at = tasks::table
                .filter(tasks::status.eq_any(TaskStatus::ACTIVE.map(TaskStatus::as_str)))
                .select(min(tasks::started_at))
                .get_result::<Option<NaiveDateTime>>(conn)
                .await?;

            Ok::<_, ApiError>(TaskMetricsSnapshot {
                counts,
                oldest_queued_at,
                oldest_active_at,
            })
        })
        .await
    }
}

impl<T> MetricsRefreshBackend for T
where
    T: BackendContext + Sync + ?Sized,
{
    async fn metrics_inventory_gauge_snapshot(&self) -> Result<InventoryGaugeSnapshot, ApiError> {
        with_connection(crate::traits::backend_pool(self), async |conn| {
            let counts = load_inventory_counts(conn).await?;
            let export_templates = export_templates::table
                .select((export_templates::id, export_templates::name))
                .order(export_templates::id)
                .load::<(i32, String)>(conn)
                .await?
                .into_iter()
                .map(|(id, name)| {
                    Ok(ExportTemplateMetricIdentity {
                        id: ExportTemplateID::new(id)?,
                        name,
                    })
                })
                .collect::<Result<Vec<_>, ApiError>>()?;

            Ok::<_, ApiError>(InventoryGaugeSnapshot {
                counts,
                export_templates,
            })
        })
        .await
    }

    async fn metrics_task_gauge_snapshot(&self) -> Result<TaskGaugeSnapshot, ApiError> {
        with_connection(crate::traits::backend_pool(self), async |conn| {
            let counts = load_task_count_rows(conn)
                .await?
                .into_iter()
                .map(|(kind, status, count)| {
                    Ok(TaskGaugeCount {
                        kind: TaskKind::from_db(&kind)?,
                        status: TaskStatus::from_db(&status)?,
                        count,
                    })
                })
                .collect::<Result<Vec<_>, ApiError>>()?;

            let oldest_queued = tasks::table
                .filter(tasks::status.eq(TaskStatus::Queued.as_str()))
                .group_by(tasks::kind)
                .select((tasks::kind, min(tasks::created_at)))
                .load::<(String, Option<NaiveDateTime>)>(conn)
                .await?;
            let oldest_active = tasks::table
                .filter(tasks::status.eq_any(TaskStatus::ACTIVE.map(TaskStatus::as_str)))
                .group_by(tasks::kind)
                .select((tasks::kind, min(tasks::started_at)))
                .load::<(String, Option<NaiveDateTime>)>(conn)
                .await?;
            let last_terminal = tasks::table
                .filter(tasks::status.eq_any(TaskStatus::TERMINAL.map(TaskStatus::as_str)))
                .group_by((tasks::kind, tasks::status))
                .select((tasks::kind, tasks::status, max(tasks::finished_at)))
                .load::<(String, String, Option<NaiveDateTime>)>(conn)
                .await?
                .into_iter()
                .map(|(kind, status, finished_at)| {
                    Ok(TaskGaugeLastTerminal {
                        kind: TaskKind::from_db(&kind)?,
                        status: TaskStatus::from_db(&status)?,
                        finished_at,
                    })
                })
                .collect::<Result<Vec<_>, ApiError>>()?;

            let mut ages_by_kind = HashMap::new();
            for (kind, timestamp) in oldest_queued {
                ages_by_kind
                    .entry(TaskKind::from_db(&kind)?)
                    .or_insert((None, None))
                    .0 = timestamp;
            }
            for (kind, timestamp) in oldest_active {
                ages_by_kind
                    .entry(TaskKind::from_db(&kind)?)
                    .or_insert((None, None))
                    .1 = timestamp;
            }
            let ages = TaskKind::ALL
                .into_iter()
                .map(|kind| {
                    let (oldest_queued_at, oldest_active_at) =
                        ages_by_kind.remove(&kind).unwrap_or((None, None));
                    TaskGaugeAge {
                        kind,
                        oldest_queued_at,
                        oldest_active_at,
                    }
                })
                .collect();

            Ok::<_, ApiError>(TaskGaugeSnapshot {
                counts,
                ages,
                last_terminal,
            })
        })
        .await
    }
}

async fn load_inventory_counts(
    conn: &mut DbConnection,
) -> Result<InventoryMetricsSnapshot, ApiError> {
    Ok(diesel::sql_query(
        r#"
        SELECT
            (SELECT COUNT(*) FROM collections) AS collections,
            (SELECT COUNT(*) FROM hubuumclass) AS classes,
            (SELECT COUNT(*) FROM hubuumobject) AS objects,
            (SELECT COUNT(*) FROM users) AS users,
            (SELECT COUNT(*) FROM groups) AS groups,
            (SELECT COUNT(*) FROM service_accounts) AS service_accounts,
            (SELECT COUNT(*) FROM remote_targets) AS remote_targets
        "#,
    )
    .get_result::<InventoryMetricsSnapshot>(conn)
    .await?)
}

async fn load_task_count_rows(
    conn: &mut DbConnection,
) -> Result<Vec<(String, String, i64)>, ApiError> {
    Ok(tasks::table
        .group_by((tasks::kind, tasks::status))
        .select((tasks::kind, tasks::status, count_star()))
        .load::<(String, String, i64)>(conn)
        .await?)
}
