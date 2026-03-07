use crate::api::openapi::{ApiErrorResponse, CountsResponse};
use crate::config::get_config;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::models::class::total_class_count;
use crate::models::namespace::total_namespace_count;
use crate::models::object::{objects_per_class_count, total_object_count};
use crate::utilities::response::json_response;
use actix_web::{Responder, ResponseError, get, http::StatusCode, web};
use diesel::QueryableByName;
use diesel::RunQueryDsl;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Nullable, Timestamp};
use serde::Serialize;
use tracing::debug;
use utoipa::ToSchema;

#[derive(Serialize, Debug, ToSchema)]
pub struct DbStateResponse {
    available_connections: u32,
    idle_connections: u32,
    active_connections: i64,
    db_size: i64,
    last_vacuum_time: Option<String>,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct TaskQueueStateResponse {
    actix_workers: usize,
    configured_task_workers: usize,
    task_poll_interval_ms: u64,
    total_tasks: i64,
    queued_tasks: i64,
    validating_tasks: i64,
    running_tasks: i64,
    active_tasks: i64,
    succeeded_tasks: i64,
    failed_tasks: i64,
    partially_succeeded_tasks: i64,
    cancelled_tasks: i64,
    import_tasks: i64,
    report_tasks: i64,
    export_tasks: i64,
    reindex_tasks: i64,
    total_task_events: i64,
    total_import_result_rows: i64,
    oldest_queued_at: Option<String>,
    oldest_active_at: Option<String>,
}

#[derive(QueryableByName, Debug)]
#[diesel(table_name = pg_stat_user_tables)]
struct DbState {
    #[diesel(sql_type = BigInt)]
    active_connections: i64,
    #[diesel(sql_type = BigInt)]
    db_size: i64,
    #[diesel(sql_type = Nullable<Timestamp>)]
    last_vacuum_time: Option<chrono::NaiveDateTime>,
}

#[derive(QueryableByName, Debug)]
struct TaskQueueState {
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
    report_tasks: i64,
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

#[utoipa::path(
    get,
    path = "/api/v0/meta/db",
    tag = "meta",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Database state", body = DbStateResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[get("db")]
pub async fn get_db_state(pool: web::Data<DbPool>, requestor: AdminAccess) -> impl Responder {
    let state = pool.state();

    let query = r#"
        SELECT
          (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_connections,
          pg_database_size(current_database()) AS db_size,
          MAX(last_vacuum) AS last_vacuum_time
        FROM 
          pg_stat_user_tables;
    "#;

    let results = match with_connection(&pool, |conn| sql_query(query).load::<DbState>(conn)) {
        Ok(results) => results,
        Err(e) => {
            return ApiError::InternalServerError(format!(
                "Error getting state for the database: {e}"
            ))
            .error_response();
        }
    };

    if let Some(row) = results.first() {
        debug!(
            message = "DB state requested",
            requestor = requestor.user.id
        );

        let response = DbStateResponse {
            available_connections: state.connections,
            idle_connections: state.idle_connections,
            active_connections: row.active_connections,
            db_size: row.db_size,
            last_vacuum_time: row.last_vacuum_time.map(|dt| dt.to_string()),
        };
        json_response(response, StatusCode::OK)
    } else {
        ApiError::InternalServerError("Error getting state for the database".to_string())
            .error_response()
    }
}

#[utoipa::path(
    get,
    path = "/api/v0/meta/counts",
    tag = "meta",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Object and class counters", body = CountsResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[get("counts")]
pub async fn get_object_and_class_count(
    pool: web::Data<DbPool>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Object count requested",
        requestor = requestor.user.id,
    );

    let response = CountsResponse {
        total_objects: total_object_count(&pool).await?,
        total_classes: total_class_count(&pool).await?,
        total_namespaces: total_namespace_count(&pool).await?,
        objects_per_class: objects_per_class_count(&pool).await?,
    };

    Ok(json_response(response, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v0/meta/tasks",
    tag = "meta",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Task queue and worker state", body = TaskQueueStateResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[get("tasks")]
pub async fn get_task_queue_state(
    pool: web::Data<DbPool>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let config = get_config()?.clone();
    let query = r#"
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
          COUNT(*) FILTER (WHERE kind = 'report')::bigint AS report_tasks,
          COUNT(*) FILTER (WHERE kind = 'export')::bigint AS export_tasks,
          COUNT(*) FILTER (WHERE kind = 'reindex')::bigint AS reindex_tasks,
          (SELECT COUNT(*) FROM task_events)::bigint AS total_task_events,
          (SELECT COUNT(*) FROM import_task_results)::bigint AS total_import_result_rows,
          MIN(created_at) FILTER (WHERE status = 'queued') AS oldest_queued_at,
          MIN(started_at) FILTER (WHERE status IN ('validating', 'running')) AS oldest_active_at
        FROM tasks;
    "#;

    let results = with_connection(&pool, |conn| sql_query(query).load::<TaskQueueState>(conn))?;
    let state = results.first().ok_or_else(|| {
        ApiError::InternalServerError("Error getting state for the task queue".to_string())
    })?;

    debug!(
        message = "Task queue state requested",
        requestor = requestor.user.id
    );

    let active_tasks = state.validating_tasks + state.running_tasks;
    let response = TaskQueueStateResponse {
        actix_workers: config.actix_workers,
        configured_task_workers: config.task_workers,
        task_poll_interval_ms: config.task_poll_interval_ms,
        total_tasks: state.total_tasks,
        queued_tasks: state.queued_tasks,
        validating_tasks: state.validating_tasks,
        running_tasks: state.running_tasks,
        active_tasks,
        succeeded_tasks: state.succeeded_tasks,
        failed_tasks: state.failed_tasks,
        partially_succeeded_tasks: state.partially_succeeded_tasks,
        cancelled_tasks: state.cancelled_tasks,
        import_tasks: state.import_tasks,
        report_tasks: state.report_tasks,
        export_tasks: state.export_tasks,
        reindex_tasks: state.reindex_tasks,
        total_task_events: state.total_task_events,
        total_import_result_rows: state.total_import_result_rows,
        oldest_queued_at: state.oldest_queued_at.map(|dt| dt.to_string()),
        oldest_active_at: state.oldest_active_at.map(|dt| dt.to_string()),
    };

    Ok(json_response(response, StatusCode::OK))
}
