use crate::db::prelude::*;
use diesel::sql_types::{BigInt, Nullable, Timestamp};

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;

#[derive(QueryableByName, Debug)]
pub struct DatabaseState {
    #[diesel(sql_type = BigInt)]
    pub active_connections: i64,
    #[diesel(sql_type = BigInt)]
    pub db_size: i64,
    #[diesel(sql_type = Nullable<Timestamp>)]
    pub last_vacuum_time: Option<chrono::NaiveDateTime>,
}

#[derive(QueryableByName, Debug)]
pub struct TaskQueueState {
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

pub async fn load_database_state(pool: &DbPool) -> Result<DatabaseState, ApiError> {
    const QUERY: &str = r#"
        SELECT
          (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_connections,
          pg_database_size(current_database()) AS db_size,
          MAX(last_vacuum) AS last_vacuum_time
        FROM pg_stat_user_tables
    "#;

    with_connection(pool, async |conn| {
        diesel::sql_query(QUERY)
            .get_result::<DatabaseState>(conn)
            .await
    })
    .await
    .map_err(|error| {
        ApiError::InternalServerError(format!("Error getting state for the database: {error}"))
    })
}

pub async fn load_task_queue_state(pool: &DbPool) -> Result<TaskQueueState, ApiError> {
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
            .get_result::<TaskQueueState>(conn)
            .await
    })
    .await
}
