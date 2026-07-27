use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::dsl::{count_star, min};
use diesel::sql_types::BigInt;

use crate::db::prelude::*;
use crate::db::with_connection;
use crate::errors::ApiError;
use crate::models::{EventDeliveryQueueHealth, EventFanoutHealth, TaskKind, TaskStatus};
use crate::schema::tasks;
use crate::traits::BackendContext;

#[derive(Debug, Clone, Copy, QueryableByName)]
struct InventoryMetricsCountRow {
    #[diesel(sql_type = BigInt)]
    collections: i64,
    #[diesel(sql_type = BigInt)]
    classes: i64,
    #[diesel(sql_type = BigInt)]
    objects: i64,
    #[diesel(sql_type = BigInt)]
    users: i64,
    #[diesel(sql_type = BigInt)]
    groups: i64,
    #[diesel(sql_type = BigInt)]
    service_accounts: i64,
    #[diesel(sql_type = BigInt)]
    remote_targets: i64,
}

#[derive(Debug, Clone)]
pub struct ExportTemplateMetricIdentity {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct InventoryMetricsSnapshot {
    pub collections: i64,
    pub classes: i64,
    pub objects: i64,
    pub users: i64,
    pub groups: i64,
    pub service_accounts: i64,
    pub remote_targets: i64,
    pub export_templates: Vec<ExportTemplateMetricIdentity>,
}

#[derive(Debug, Clone)]
pub struct TaskMetricsCount {
    pub kind: String,
    pub status: String,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct TaskMetricsAge {
    pub kind: String,
    pub oldest_queued_at: Option<NaiveDateTime>,
    pub oldest_active_at: Option<NaiveDateTime>,
}

#[derive(Debug, Clone)]
pub struct TaskMetricsSnapshot {
    pub counts: Vec<TaskMetricsCount>,
    pub ages: Vec<TaskMetricsAge>,
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

impl<T> MetricsBackend for T
where
    T: BackendContext + Sync + ?Sized,
{
    async fn metrics_inventory_snapshot(&self) -> Result<InventoryMetricsSnapshot, ApiError> {
        with_connection(self.db_pool(), async |conn| {
            let counts = diesel::sql_query(
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
            .get_result::<InventoryMetricsCountRow>(conn)
            .await?;
            let export_templates = crate::schema::export_templates::table
                .select((
                    crate::schema::export_templates::id,
                    crate::schema::export_templates::name,
                ))
                .order(crate::schema::export_templates::id)
                .load::<(i32, String)>(conn)
                .await?
                .into_iter()
                .map(|(id, name)| ExportTemplateMetricIdentity { id, name })
                .collect();

            Ok::<_, diesel::result::Error>(InventoryMetricsSnapshot {
                collections: counts.collections,
                classes: counts.classes,
                objects: counts.objects,
                users: counts.users,
                groups: counts.groups,
                service_accounts: counts.service_accounts,
                remote_targets: counts.remote_targets,
                export_templates,
            })
        })
        .await
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskMetricsSnapshot, ApiError> {
        with_connection(self.db_pool(), async |conn| {
            let counts = tasks::table
                .group_by((tasks::kind, tasks::status))
                .select((tasks::kind, tasks::status, count_star()))
                .load::<(String, String, i64)>(conn)
                .await?
                .into_iter()
                .map(|(kind, status, count)| TaskMetricsCount {
                    kind,
                    status,
                    count,
                })
                .collect();

            let oldest_queued = tasks::table
                .filter(tasks::status.eq(TaskStatus::Queued.as_str()))
                .group_by(tasks::kind)
                .select((tasks::kind, min(tasks::created_at)))
                .load::<(String, Option<NaiveDateTime>)>(conn)
                .await?;

            let oldest_active = tasks::table
                .filter(tasks::status.eq_any([
                    TaskStatus::Validating.as_str(),
                    TaskStatus::Running.as_str(),
                ]))
                .group_by(tasks::kind)
                .select((tasks::kind, min(tasks::started_at)))
                .load::<(String, Option<NaiveDateTime>)>(conn)
                .await?;

            let mut ages_by_kind = HashMap::new();
            for (kind, timestamp) in oldest_queued {
                ages_by_kind.entry(kind).or_insert((None, None)).0 = timestamp;
            }
            for (kind, timestamp) in oldest_active {
                ages_by_kind.entry(kind).or_insert((None, None)).1 = timestamp;
            }
            let ages = TaskKind::ALL
                .into_iter()
                .map(|kind| {
                    let kind = kind.as_str();
                    let (oldest_queued_at, oldest_active_at) =
                        ages_by_kind.remove(kind).unwrap_or((None, None));
                    TaskMetricsAge {
                        kind: kind.to_string(),
                        oldest_queued_at,
                        oldest_active_at,
                    }
                })
                .collect();

            Ok::<_, diesel::result::Error>(TaskMetricsSnapshot { counts, ages })
        })
        .await
    }
}
