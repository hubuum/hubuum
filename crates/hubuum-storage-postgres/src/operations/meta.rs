use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamp};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, ExportTemplateId};
use hubuum_storage_core::{
    StorageOperationalExportTemplateAuditEntry, StorageOperationalExportTemplateHealth,
    StorageOperationalTaskActiveCounts, StorageOperationalTaskKindCounts,
    StorageOperationalTaskQueueSnapshot, StorageOperationalTaskStatusCounts,
    StorageOperationalTaskTerminalCounts,
};

use crate::{PostgresRuntime, PostgresStorageError, PostgresStorageSnapshot};

#[derive(QueryableByName, Debug)]
struct DatabaseStateRow {
    #[diesel(sql_type = BigInt)]
    active_connections: i64,
    #[diesel(sql_type = BigInt)]
    db_size: i64,
    #[diesel(sql_type = Nullable<Timestamp>)]
    last_vacuum_time: Option<chrono::NaiveDateTime>,
}

#[derive(QueryableByName, Debug)]
struct TaskQueueStateRow {
    #[diesel(sql_type = BigInt)]
    total_tasks: i64,
    #[diesel(sql_type = BigInt)]
    queued_tasks: i64,
    #[diesel(sql_type = BigInt)]
    validating_tasks: i64,
    #[diesel(sql_type = BigInt)]
    running_tasks: i64,
    #[diesel(sql_type = BigInt)]
    succeeded_tasks: i64,
    #[diesel(sql_type = BigInt)]
    failed_tasks: i64,
    #[diesel(sql_type = BigInt)]
    partially_succeeded_tasks: i64,
    #[diesel(sql_type = BigInt)]
    cancelled_tasks: i64,
    #[diesel(sql_type = BigInt)]
    import_tasks: i64,
    #[diesel(sql_type = BigInt)]
    export_tasks: i64,
    #[diesel(sql_type = BigInt)]
    reindex_tasks: i64,
    #[diesel(sql_type = BigInt)]
    total_task_events: i64,
    #[diesel(sql_type = BigInt)]
    total_import_result_rows: i64,
    #[diesel(sql_type = Nullable<Timestamp>)]
    oldest_queued_at: Option<chrono::NaiveDateTime>,
    #[diesel(sql_type = Nullable<Timestamp>)]
    oldest_active_at: Option<chrono::NaiveDateTime>,
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

pub async fn load_storage_snapshot(
    runtime: &PostgresRuntime,
) -> Result<PostgresStorageSnapshot, PostgresStorageError> {
    const QUERY: &str = r#"
        SELECT
          (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_connections,
          pg_database_size(current_database()) AS db_size,
          MAX(last_vacuum) AS last_vacuum_time
        FROM pg_stat_user_tables
    "#;

    runtime
        .with_connection(async |connection| {
            diesel::sql_query(QUERY)
                .get_result::<DatabaseStateRow>(connection)
                .await
        })
        .await
        .map(|row| {
            PostgresStorageSnapshot::new(row.active_connections, row.db_size, row.last_vacuum_time)
        })
}

pub async fn load_task_queue_snapshot(
    runtime: &PostgresRuntime,
) -> Result<StorageOperationalTaskQueueSnapshot, PostgresStorageError> {
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

    let row = runtime
        .with_connection(async |connection| {
            diesel::sql_query(QUERY)
                .get_result::<TaskQueueStateRow>(connection)
                .await
        })
        .await?;
    let active = crate::validate_persisted(
        "active task counts",
        StorageOperationalTaskActiveCounts::try_new(
            row.queued_tasks,
            row.validating_tasks,
            row.running_tasks,
        ),
    )?;
    let terminal = crate::validate_persisted(
        "terminal task counts",
        StorageOperationalTaskTerminalCounts::try_new(
            row.succeeded_tasks,
            row.failed_tasks,
            row.partially_succeeded_tasks,
            row.cancelled_tasks,
        ),
    )?;
    let statuses = crate::validate_persisted(
        "task status counts",
        StorageOperationalTaskStatusCounts::try_new(row.total_tasks, active, terminal),
    )?;
    let kinds = crate::validate_persisted(
        "task kind counts",
        StorageOperationalTaskKindCounts::try_new(
            row.import_tasks,
            row.export_tasks,
            row.reindex_tasks,
        ),
    )?;
    crate::validate_persisted(
        "task queue snapshot",
        StorageOperationalTaskQueueSnapshot::try_new(
            statuses,
            kinds,
            row.total_task_events,
            row.total_import_result_rows,
            row.oldest_queued_at.map(|timestamp| timestamp.and_utc()),
            row.oldest_active_at.map(|timestamp| timestamp.and_utc()),
        ),
    )
}

pub async fn load_export_template_health(
    runtime: &PostgresRuntime,
) -> Result<Vec<StorageOperationalExportTemplateHealth>, PostgresStorageError> {
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

    let rows = runtime
        .with_connection(async |connection| {
            diesel::sql_query(QUERY)
                .load::<ExportTemplateHealthRow>(connection)
                .await
        })
        .await?;
    rows.into_iter()
        .map(|row| {
            crate::validate_persisted(
                "export template health snapshot",
                StorageOperationalExportTemplateHealth::try_new(
                    row.template_name,
                    row.runs,
                    row.warning_total,
                    row.warning_max,
                    row.total_duration_ms_total,
                    row.total_duration_ms_max,
                ),
            )
        })
        .collect()
}

pub async fn load_export_templates_for_audit(
    runtime: &PostgresRuntime,
) -> Result<Vec<StorageOperationalExportTemplateAuditEntry>, PostgresStorageError> {
    const QUERY: &str = r#"
        SELECT id, collection_id, name, template, content_type
        FROM export_templates
        ORDER BY collection_id, id
    "#;

    runtime
        .with_connection(async |connection| {
            diesel::sql_query(QUERY)
                .load::<ExportTemplateAuditRow>(connection)
                .await
        })
        .await
        .and_then(|rows| {
            rows.into_iter()
                .map(|row| {
                    Ok(StorageOperationalExportTemplateAuditEntry::new(
                        ExportTemplateId::new(row.id)?,
                        CollectionId::new(row.collection_id)?,
                        row.name,
                        row.template,
                        row.content_type,
                    ))
                })
                .collect()
        })
}
