use chrono::NaiveDateTime;
use diesel::dsl::{count_star, min};
use diesel::sql_types::BigInt;

use crate::db::prelude::*;
use crate::db::with_connection;
use crate::errors::ApiError;
use crate::models::{EventDeliveryQueueHealth, EventFanoutHealth, TaskStatus};
use crate::schema::tasks;
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
            diesel::sql_query(
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
            .await
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

            let oldest_queued_at = tasks::table
                .filter(tasks::status.eq(TaskStatus::Queued.as_str()))
                .select(min(tasks::created_at))
                .get_result::<Option<NaiveDateTime>>(conn)
                .await?;

            let oldest_active_at = tasks::table
                .filter(tasks::status.eq_any([
                    TaskStatus::Validating.as_str(),
                    TaskStatus::Running.as_str(),
                ]))
                .select(min(tasks::started_at))
                .get_result::<Option<NaiveDateTime>>(conn)
                .await?;

            Ok::<_, diesel::result::Error>(TaskMetricsSnapshot {
                counts,
                oldest_queued_at,
                oldest_active_at,
            })
        })
        .await
    }
}
