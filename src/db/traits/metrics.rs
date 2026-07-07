use chrono::NaiveDateTime;
use diesel::dsl::{count_star, min};
use diesel::prelude::*;

use crate::db::with_connection;
use crate::errors::ApiError;
use crate::models::TaskStatus;
use crate::schema::{
    collections, groups, hubuumclass, hubuumobject, remote_targets, service_accounts, tasks, users,
};
use crate::traits::BackendContext;

#[derive(Debug, Clone, Copy)]
pub struct InventoryMetricsSnapshot {
    pub collections: i64,
    pub classes: i64,
    pub objects: i64,
    pub users: i64,
    pub groups: i64,
    pub service_accounts: i64,
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

pub trait MetricsBackend {
    async fn metrics_inventory_snapshot(&self) -> Result<InventoryMetricsSnapshot, ApiError>;
    async fn metrics_task_snapshot(&self) -> Result<TaskMetricsSnapshot, ApiError>;
}

impl<T> MetricsBackend for T
where
    T: BackendContext + Sync + ?Sized,
{
    async fn metrics_inventory_snapshot(&self) -> Result<InventoryMetricsSnapshot, ApiError> {
        with_connection(self.db_pool(), |conn| {
            Ok::<_, diesel::result::Error>(InventoryMetricsSnapshot {
                collections: collections::table.select(count_star()).get_result(conn)?,
                classes: hubuumclass::table.select(count_star()).get_result(conn)?,
                objects: hubuumobject::table.select(count_star()).get_result(conn)?,
                users: users::table.select(count_star()).get_result(conn)?,
                groups: groups::table.select(count_star()).get_result(conn)?,
                service_accounts: service_accounts::table
                    .select(count_star())
                    .get_result(conn)?,
                remote_targets: remote_targets::table
                    .select(count_star())
                    .get_result(conn)?,
            })
        })
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskMetricsSnapshot, ApiError> {
        with_connection(self.db_pool(), |conn| {
            let counts = tasks::table
                .group_by((tasks::kind, tasks::status))
                .select((tasks::kind, tasks::status, count_star()))
                .load::<(String, String, i64)>(conn)?
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
                .get_result::<Option<NaiveDateTime>>(conn)?;

            let oldest_active_at = tasks::table
                .filter(tasks::status.eq_any([
                    TaskStatus::Validating.as_str(),
                    TaskStatus::Running.as_str(),
                ]))
                .select(min(tasks::started_at))
                .get_result::<Option<NaiveDateTime>>(conn)?;

            Ok::<_, diesel::result::Error>(TaskMetricsSnapshot {
                counts,
                oldest_queued_at,
                oldest_active_at,
            })
        })
    }
}
