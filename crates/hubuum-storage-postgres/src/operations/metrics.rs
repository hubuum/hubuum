use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::QueryableByName;
use diesel::dsl::{count_star, max, min};
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::sql_types::BigInt;
use diesel_async::RunQueryDsl;
use hubuum_domain::ExportTemplateId;
use hubuum_storage_core::{
    ExportTemplateMetricIdentity, InventoryGaugeSnapshot, InventoryMetricsSnapshot,
    StoragePoolAcquisitionState, StoragePoolCapacity, StoragePoolConnectionState, StoragePoolState,
    StorageTaskKind, StorageTaskStatus, TaskGaugeAge, TaskGaugeCount, TaskGaugeLastTerminal,
    TaskGaugeSnapshot,
};

use crate::schema::{export_templates, tasks};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

/// Return the backend-neutral projection of the PostgreSQL connection pool.
///
/// Pool implementation details stay inside the adapter even though collecting
/// this snapshot does not require an asynchronous database call.
#[must_use]
pub fn pool_state(runtime: &PostgresRuntime) -> StoragePoolState {
    let pool = runtime.pool();
    let state = pool.state();
    let max_connections = pool.config().max_size;
    let in_use_connections = state.connections.saturating_sub(state.idle_connections);

    StoragePoolState::new(
        StoragePoolCapacity::new(
            max_connections,
            state.connections,
            max_connections.saturating_sub(in_use_connections),
            state.idle_connections,
            in_use_connections,
        ),
        StoragePoolAcquisitionState::new(
            state.statistics.pending_gets(),
            state.statistics.get_started,
            state.statistics.get_direct,
            state.statistics.get_waited,
            state.statistics.get_timed_out,
            u64::try_from(state.statistics.get_wait_time.as_millis()).unwrap_or(u64::MAX),
        ),
        StoragePoolConnectionState::new(
            state.statistics.connections_created,
            state.statistics.connections_closed_broken,
            state.statistics.connections_closed_invalid,
            state.statistics.connections_closed_max_lifetime,
            state.statistics.connections_closed_idle_timeout,
        ),
    )
}

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

fn storage_task_kind(value: &str) -> Result<StorageTaskKind, PostgresStorageError> {
    StorageTaskKind::from_persisted(value).ok_or_else(|| {
        PostgresStorageError::database(format!("Unknown persisted task kind '{value}'"))
    })
}

fn storage_task_status(value: &str) -> Result<StorageTaskStatus, PostgresStorageError> {
    StorageTaskStatus::from_persisted(value).ok_or_else(|| {
        PostgresStorageError::database(format!("Unknown persisted task status '{value}'"))
    })
}

pub async fn load_inventory_gauge_snapshot(
    runtime: &PostgresRuntime,
) -> Result<InventoryGaugeSnapshot, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            let counts = load_inventory_counts(connection).await?;
            let export_templates = export_templates::table
                .select((export_templates::id, export_templates::name))
                .order(export_templates::id)
                .load::<(i32, String)>(connection)
                .await?
                .into_iter()
                .map(|(id, name)| {
                    Ok(ExportTemplateMetricIdentity::new(
                        ExportTemplateId::new(id)?,
                        name,
                    ))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()?;

            Ok::<_, PostgresStorageError>(InventoryGaugeSnapshot::new(counts, export_templates))
        })
        .await
}

pub async fn load_task_gauge_snapshot(
    runtime: &PostgresRuntime,
) -> Result<TaskGaugeSnapshot, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            let counts = load_task_count_rows(connection)
                .await?
                .into_iter()
                .map(|(kind, status, count)| {
                    Ok(TaskGaugeCount::new(
                        storage_task_kind(&kind)?,
                        storage_task_status(&status)?,
                        count,
                    ))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()?;

            let oldest_queued = tasks::table
                .filter(tasks::status.eq(StorageTaskStatus::Queued.as_str()))
                .group_by(tasks::kind)
                .select((tasks::kind, min(tasks::created_at)))
                .load::<(String, Option<NaiveDateTime>)>(connection)
                .await?;
            let oldest_active = tasks::table
                .filter(
                    tasks::status.eq_any(StorageTaskStatus::ACTIVE.map(StorageTaskStatus::as_str)),
                )
                .group_by(tasks::kind)
                .select((tasks::kind, min(tasks::started_at)))
                .load::<(String, Option<NaiveDateTime>)>(connection)
                .await?;
            let last_terminal = tasks::table
                .filter(
                    tasks::status
                        .eq_any(StorageTaskStatus::TERMINAL.map(StorageTaskStatus::as_str)),
                )
                .group_by((tasks::kind, tasks::status))
                .select((tasks::kind, tasks::status, max(tasks::finished_at)))
                .load::<(String, String, Option<NaiveDateTime>)>(connection)
                .await?
                .into_iter()
                .map(|(kind, status, finished_at)| {
                    Ok(TaskGaugeLastTerminal::new(
                        storage_task_kind(&kind)?,
                        storage_task_status(&status)?,
                        finished_at,
                    ))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()?;

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

            Ok::<_, PostgresStorageError>(TaskGaugeSnapshot::new(counts, ages, last_terminal))
        })
        .await
}

async fn load_inventory_counts(
    connection: &mut PostgresConnection,
) -> Result<InventoryMetricsSnapshot, PostgresStorageError> {
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
    .get_result::<InventoryMetricsRow>(connection)
    .await?;
    Ok(row.into())
}

async fn load_task_count_rows(
    connection: &mut PostgresConnection,
) -> Result<Vec<(String, String, i64)>, PostgresStorageError> {
    Ok(tasks::table
        .group_by((tasks::kind, tasks::status))
        .select((tasks::kind, tasks::status, count_star()))
        .load::<(String, String, i64)>(connection)
        .await?)
}
