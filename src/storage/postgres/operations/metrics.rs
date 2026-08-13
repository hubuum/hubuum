use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::dsl::{count_star, max, min};
use diesel::sql_types::BigInt;

use crate::errors::ApiError;
use crate::models::TaskStatus;
use crate::schema::{export_templates, tasks};
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresConnection, PostgresPool, with_connection};
use crate::storage::{
    ExportTemplateMetricIdentity, InventoryGaugeSnapshot, InventoryMetricsSnapshot,
    StorageTaskKind, StorageTaskStatus, TaskGaugeAge, TaskGaugeCount, TaskGaugeLastTerminal,
    TaskGaugeSnapshot,
};

#[derive(Debug, Clone, Copy, QueryableByName)]
struct InventoryMetricsRow {
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

impl From<InventoryMetricsRow> for InventoryMetricsSnapshot {
    fn from(row: InventoryMetricsRow) -> Self {
        Self::new(
            row.collections,
            row.classes,
            row.objects,
            row.users,
            row.groups,
            row.service_accounts,
            row.remote_targets,
        )
    }
}

fn storage_task_kind(value: &str) -> Result<StorageTaskKind, ApiError> {
    StorageTaskKind::from_persisted(value)
        .ok_or_else(|| ApiError::DatabaseError(format!("Unknown persisted task kind '{value}'")))
}

fn storage_task_status(value: &str) -> Result<StorageTaskStatus, ApiError> {
    StorageTaskStatus::from_persisted(value)
        .ok_or_else(|| ApiError::DatabaseError(format!("Unknown persisted task status '{value}'")))
}

pub(crate) async fn load_inventory_gauge_snapshot(
    pool: &PostgresPool,
) -> Result<InventoryGaugeSnapshot, ApiError> {
    with_connection(pool, async |conn| {
        let counts = load_inventory_counts(conn).await?;
        let export_templates = export_templates::table
            .select((export_templates::id, export_templates::name))
            .order(export_templates::id)
            .load::<(i32, String)>(conn)
            .await?
            .into_iter()
            .map(|(id, name)| {
                ExportTemplateMetricIdentity::new(id, name).ok_or_else(|| {
                    ApiError::DatabaseError(format!("Invalid persisted export template id '{id}'"))
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;

        Ok::<_, ApiError>(InventoryGaugeSnapshot::new(counts, export_templates))
    })
    .await
}

pub(crate) async fn load_task_gauge_snapshot(
    pool: &PostgresPool,
) -> Result<TaskGaugeSnapshot, ApiError> {
    with_connection(pool, async |conn| {
        let counts = load_task_count_rows(conn)
            .await?
            .into_iter()
            .map(|(kind, status, count)| {
                Ok(TaskGaugeCount::new(
                    storage_task_kind(&kind)?,
                    storage_task_status(&status)?,
                    count,
                ))
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
                Ok(TaskGaugeLastTerminal::new(
                    storage_task_kind(&kind)?,
                    storage_task_status(&status)?,
                    finished_at,
                ))
            })
            .collect::<Result<Vec<_>, ApiError>>()?;

        let mut ages_by_kind = HashMap::new();
        for (kind, timestamp) in oldest_queued {
            ages_by_kind
                .entry(storage_task_kind(&kind)?)
                .or_insert((None, None))
                .0 = timestamp;
        }
        for (kind, timestamp) in oldest_active {
            ages_by_kind
                .entry(storage_task_kind(&kind)?)
                .or_insert((None, None))
                .1 = timestamp;
        }
        let ages = StorageTaskKind::ALL
            .into_iter()
            .map(|kind| {
                let (oldest_queued_at, oldest_active_at) =
                    ages_by_kind.remove(&kind).unwrap_or((None, None));
                TaskGaugeAge::new(kind, oldest_queued_at, oldest_active_at)
            })
            .collect();

        Ok::<_, ApiError>(TaskGaugeSnapshot::new(counts, ages, last_terminal))
    })
    .await
}

async fn load_inventory_counts(
    conn: &mut PostgresConnection,
) -> Result<InventoryMetricsSnapshot, ApiError> {
    let row = diesel::sql_query(
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
    .get_result::<InventoryMetricsRow>(conn)
    .await?;
    Ok(row.into())
}

async fn load_task_count_rows(
    conn: &mut PostgresConnection,
) -> Result<Vec<(String, String, i64)>, ApiError> {
    Ok(tasks::table
        .group_by((tasks::kind, tasks::status))
        .select((tasks::kind, tasks::status, count_star()))
        .load::<(String, String, i64)>(conn)
        .await?)
}
