use crate::storage::postgres::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamp};

use crate::errors::ApiError;
use crate::storage::postgres::with_connection;
use crate::storage::{
    OperationalExportTemplateAuditEntry, OperationalExportTemplateHealth,
    OperationalStorageSnapshot, OperationalTaskActiveCounts, OperationalTaskKindCounts,
    OperationalTaskQueueSnapshot, OperationalTaskStatusCounts, OperationalTaskTerminalCounts,
};

#[derive(QueryableByName, Debug)]
struct DatabaseStateRow {
    #[diesel(sql_type = BigInt)]
    pub active_connections: i64,
    #[diesel(sql_type = BigInt)]
    pub db_size: i64,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub last_vacuum_time: Option<chrono::NaiveDateTime>,
}

#[derive(QueryableByName, Debug)]
struct TaskQueueStateRow {
    #[diesel(sql_type = BigInt)]
    pub total_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub queued_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub validating_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub running_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub succeeded_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub failed_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub partially_succeeded_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub cancelled_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub import_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub export_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub reindex_tasks: i64,
    #[diesel(sql_type = BigInt)]
    pub total_task_events: i64,
    #[diesel(sql_type = BigInt)]
    pub total_import_result_rows: i64,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub oldest_queued_at: Option<chrono::NaiveDateTime>,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub oldest_active_at: Option<chrono::NaiveDateTime>,
}

#[derive(QueryableByName, Debug)]
struct ExportTemplateHealthRow {
    #[diesel(sql_type = Nullable<Text>)]
    template_name: Option<String>,
    #[diesel(sql_type = BigInt)]
    runs: i64,
    #[diesel(sql_type = BigInt)]
    warning_total: i64,
    #[diesel(sql_type = Integer)]
    warning_max: i32,
    #[diesel(sql_type = BigInt)]
    total_duration_ms_total: i64,
    #[diesel(sql_type = Integer)]
    total_duration_ms_max: i32,
}

#[derive(QueryableByName, Debug)]
struct ExportTemplateAuditRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Integer)]
    collection_id: i32,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(sql_type = Text)]
    template: String,
    #[diesel(sql_type = Text)]
    content_type: String,
}

pub(crate) async fn load_storage_snapshot(
    pool: &impl crate::storage::StorageContext,
) -> Result<OperationalStorageSnapshot, ApiError> {
    const QUERY: &str = r#"
        SELECT
          (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_connections,
          pg_database_size(current_database()) AS db_size,
          MAX(last_vacuum) AS last_vacuum_time
        FROM pg_stat_user_tables
    "#;

    with_connection(pool, async |conn| {
        diesel::sql_query(QUERY)
            .get_result::<DatabaseStateRow>(conn)
            .await
    })
    .await
    .map(|row| {
        OperationalStorageSnapshot::new(row.active_connections, row.db_size, row.last_vacuum_time)
    })
    .map_err(|error| ApiError::InternalServerError(format!("Error getting storage state: {error}")))
}

pub(crate) async fn load_task_queue_snapshot(
    pool: &impl crate::storage::StorageContext,
) -> Result<OperationalTaskQueueSnapshot, ApiError> {
    const QUERY: &str = r#"
        SELECT
          COUNT(*)::bigint AS total_tasks,
          COUNT(*) FILTER (WHERE status = 'queued')::bigint AS queued_tasks,
          COUNT(*) FILTER (WHERE status = 'validating')::bigint AS validating_tasks,
          COUNT(*) FILTER (WHERE status = 'running')::bigint AS running_tasks,
          COUNT(*) FILTER (WHERE status = 'succeeded')::bigint AS succeeded_tasks,
          COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed_tasks,
          COUNT(*) FILTER (WHERE status = 'partially_succeeded')::bigint AS partially_succeeded_tasks,
          COUNT(*) FILTER (WHERE status = 'cancelled')::bigint AS cancelled_tasks,
          COUNT(*) FILTER (WHERE kind = 'import')::bigint AS import_tasks,
          COUNT(*) FILTER (WHERE kind = 'export')::bigint AS export_tasks,
          COUNT(*) FILTER (WHERE kind = 'reindex')::bigint AS reindex_tasks,
          (SELECT COUNT(*) FROM events WHERE entity_type = 'task')::bigint AS total_task_events,
          (SELECT COUNT(*) FROM import_task_results)::bigint AS total_import_result_rows,
          MIN(created_at) FILTER (WHERE status = 'queued') AS oldest_queued_at,
          MIN(started_at) FILTER (WHERE status IN ('validating', 'running')) AS oldest_active_at
        FROM tasks
    "#;

    with_connection(pool, async |conn| {
        diesel::sql_query(QUERY)
            .get_result::<TaskQueueStateRow>(conn)
            .await
    })
    .await
    .map(|row| {
        let statuses = OperationalTaskStatusCounts::new(
            row.total_tasks,
            OperationalTaskActiveCounts::new(
                row.queued_tasks,
                row.validating_tasks,
                row.running_tasks,
            ),
            OperationalTaskTerminalCounts::new(
                row.succeeded_tasks,
                row.failed_tasks,
                row.partially_succeeded_tasks,
                row.cancelled_tasks,
            ),
        );
        let kinds =
            OperationalTaskKindCounts::new(row.import_tasks, row.export_tasks, row.reindex_tasks);
        OperationalTaskQueueSnapshot::new(
            statuses,
            kinds,
            row.total_task_events,
            row.total_import_result_rows,
            row.oldest_queued_at,
            row.oldest_active_at,
        )
    })
}

pub(crate) async fn load_export_template_health(
    pool: &impl crate::storage::StorageContext,
) -> Result<Vec<OperationalExportTemplateHealth>, ApiError> {
    const QUERY: &str = r#"
        SELECT
          template_name,
          COUNT(*)::bigint AS runs,
          SUM(warning_count)::bigint AS warning_total,
          MAX(warning_count)::integer AS warning_max,
          SUM(total_duration_ms)::bigint AS total_duration_ms_total,
          MAX(total_duration_ms)::integer AS total_duration_ms_max
        FROM export_task_outputs
        GROUP BY template_name
        ORDER BY template_name ASC NULLS FIRST
    "#;

    with_connection(pool, async |connection| {
        diesel::sql_query(QUERY)
            .load::<ExportTemplateHealthRow>(connection)
            .await
    })
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| {
                OperationalExportTemplateHealth::new(
                    row.template_name,
                    row.runs,
                    row.warning_total,
                    row.warning_max,
                    row.total_duration_ms_total,
                    row.total_duration_ms_max,
                )
            })
            .collect()
    })
}

pub(crate) async fn load_export_templates_for_audit(
    pool: &impl crate::storage::StorageContext,
) -> Result<Vec<OperationalExportTemplateAuditEntry>, ApiError> {
    const QUERY: &str = r#"
        SELECT id, collection_id, name, template, content_type
        FROM export_templates
        ORDER BY collection_id, id
    "#;

    with_connection(pool, async |connection| {
        diesel::sql_query(QUERY)
            .load::<ExportTemplateAuditRow>(connection)
            .await
    })
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| {
                OperationalExportTemplateAuditEntry::new(
                    row.id,
                    row.collection_id,
                    row.name,
                    row.template,
                    row.content_type,
                )
            })
            .collect()
    })
}
