use crate::api::openapi::{ApiErrorResponse, CountsResponse};
use crate::api::response::ApiResponse;
use crate::config::{get_config, login_rate_limit_config};
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::middlewares::rate_limit;
use crate::models::class::total_class_count;
use crate::models::collection::total_collection_count;
use crate::models::object::{objects_per_class_count, total_object_count};
use actix_web::{Responder, delete, get, http::StatusCode, web};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use diesel::QueryableByName;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Nullable, Timestamp};
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::ToSchema;

#[derive(Serialize, Debug, ToSchema)]
pub struct DbStateResponse {
    /// Configured maximum number of connections in this process-local pool.
    max_connections: u32,
    /// Connections currently managed by this process-local pool.
    total_connections: u32,
    /// Remaining capacity: idle connections plus capacity to create connections.
    available_connections: u32,
    /// Established connections currently waiting in the pool.
    idle_connections: u32,
    /// Established connections currently checked out of the pool.
    in_use_connections: u32,
    /// Connection acquisitions currently waiting to complete.
    pending_acquisitions: u64,
    /// Cumulative connection acquisitions started.
    acquisitions_started: u64,
    /// Cumulative acquisitions completed without waiting.
    acquisitions_direct: u64,
    /// Cumulative acquisitions that waited for a connection.
    acquisitions_waited: u64,
    /// Cumulative acquisitions that exceeded the acquisition timeout.
    acquisitions_timed_out: u64,
    /// Cumulative time spent waiting for connections, in milliseconds.
    acquisition_wait_time_ms: u64,
    /// Cumulative connections established by the pool.
    connections_created: u64,
    /// Cumulative connections discarded because they were broken.
    connections_closed_broken: u64,
    /// Cumulative connections discarded after checkout validation failed.
    connections_closed_invalid: u64,
    /// Cumulative connections closed after reaching their maximum lifetime.
    connections_closed_max_lifetime: u64,
    /// Cumulative connections closed after reaching the idle timeout.
    connections_closed_idle_timeout: u64,
    /// Active PostgreSQL sessions reported by `pg_stat_activity`.
    active_connections: i64,
    /// Current database size in bytes.
    db_size: i64,
    /// Most recent vacuum timestamp among user tables.
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
pub async fn get_db_state(
    pool: web::Data<DbPool>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let query = r#"
        SELECT
          (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_connections,
          pg_database_size(current_database()) AS db_size,
          MAX(last_vacuum) AS last_vacuum_time
        FROM 
          pg_stat_user_tables;
    "#;

    let results = match with_connection(&pool, async |conn| {
        sql_query(query).load::<DbState>(conn).await
    })
    .await
    {
        Ok(results) => results,
        Err(e) => {
            return Err(ApiError::InternalServerError(format!(
                "Error getting state for the database: {e}"
            )));
        }
    };

    if let Some(row) = results.as_slice().first() {
        let state = pool.state();
        let max_connections = pool.config().max_size;
        let in_use_connections = state.connections.saturating_sub(state.idle_connections);
        let available_connections = max_connections.saturating_sub(in_use_connections);
        let acquisition_wait_time_ms =
            u64::try_from(state.statistics.get_wait_time.as_millis()).unwrap_or(u64::MAX);
        debug!(
            message = "DB state requested",
            requestor = requestor.user.id
        );

        let response = DbStateResponse {
            max_connections,
            total_connections: state.connections,
            available_connections,
            idle_connections: state.idle_connections,
            in_use_connections,
            pending_acquisitions: state.statistics.pending_gets(),
            acquisitions_started: state.statistics.get_started,
            acquisitions_direct: state.statistics.get_direct,
            acquisitions_waited: state.statistics.get_waited,
            acquisitions_timed_out: state.statistics.get_timed_out,
            acquisition_wait_time_ms,
            connections_created: state.statistics.connections_created,
            connections_closed_broken: state.statistics.connections_closed_broken,
            connections_closed_invalid: state.statistics.connections_closed_invalid,
            connections_closed_max_lifetime: state.statistics.connections_closed_max_lifetime,
            connections_closed_idle_timeout: state.statistics.connections_closed_idle_timeout,
            active_connections: row.active_connections,
            db_size: row.db_size,
            last_vacuum_time: row.last_vacuum_time.map(|dt| dt.to_string()),
        };
        Ok(ApiResponse::new(response, StatusCode::OK))
    } else {
        Err(ApiError::InternalServerError(
            "Error getting state for the database".to_string(),
        ))
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
        total_collections: total_collection_count(&pool).await?,
        objects_per_class: objects_per_class_count(&pool).await?,
    };

    Ok(ApiResponse::new(response, StatusCode::OK))
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
          COUNT(*) FILTER (WHERE kind = 'export')::bigint AS export_tasks,
          COUNT(*) FILTER (WHERE kind = 'reindex')::bigint AS reindex_tasks,
          (SELECT COUNT(*) FROM events WHERE entity_type = 'task')::bigint AS total_task_events,
          (SELECT COUNT(*) FROM import_task_results)::bigint AS total_import_result_rows,
          MIN(created_at) FILTER (WHERE status = 'queued') AS oldest_queued_at,
          MIN(started_at) FILTER (WHERE status IN ('validating', 'running')) AS oldest_active_at
        FROM tasks;
    "#;

    let results = with_connection(&pool, async |conn| {
        sql_query(query).load::<TaskQueueState>(conn).await
    })
    .await?;
    let state = results.as_slice().first().ok_or_else(|| {
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
        export_tasks: state.export_tasks,
        reindex_tasks: state.reindex_tasks,
        total_task_events: state.total_task_events,
        total_import_result_rows: state.total_import_result_rows,
        oldest_queued_at: state.oldest_queued_at.map(|dt| dt.to_string()),
        oldest_active_at: state.oldest_active_at.map(|dt| dt.to_string()),
    };

    Ok(ApiResponse::new(response, StatusCode::OK))
}

/// Effective login rate-limit configuration, echoed back in the admin state endpoint.
#[derive(Serialize, Debug, ToSchema)]
pub struct LoginRateLimitConfigResponse {
    enabled: bool,
    backend: String,
    max_attempts: usize,
    max_attempts_per_ip: usize,
    max_attempts_per_subnet: usize,
    window_seconds: u64,
    backoff_base_seconds: u64,
    backoff_max_seconds: u64,
    subnet_prefix_v4: u8,
    subnet_prefix_v6: u8,
}

/// One tracked rate-limit scope (a user+IP pair, an IP, or a subnet).
#[derive(Serialize, Debug, ToSchema)]
pub struct LoginRateLimitEntryResponse {
    /// Opaque, URL-safe id; pass to `DELETE /meta/login-rate-limit/{id}` to release it.
    id: String,
    /// Scope kind: `user_ip`, `ip`, or `subnet`.
    scope: String,
    /// Human-readable identifier for the scope.
    identifier: String,
    /// Failed attempts currently within the sliding window.
    attempts: usize,
    /// Whether the scope is locked out right now.
    locked: bool,
    /// Remaining lockout time in seconds, if currently locked.
    locked_for_seconds: Option<u64>,
    /// Current exponential-backoff level.
    lockout_level: u32,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct LoginRateLimitStateResponse {
    config: LoginRateLimitConfigResponse,
    /// Total number of tracked scopes (before filtering).
    tracked_entries: usize,
    /// Number of scopes currently locked out (before filtering).
    locked_entries: usize,
    /// Number of scopes returned in `entries` after applying the query filters.
    returned_entries: usize,
    /// The tracked scopes matching the `include`, `scope`, and `q` query parameters.
    entries: Vec<LoginRateLimitEntryResponse>,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct ReleaseRateLimitResponse {
    released: bool,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct ClearRateLimitResponse {
    cleared: usize,
}

#[derive(Deserialize, Debug)]
pub struct LoginRateLimitQuery {
    /// `locked` (default) returns only locked scopes; `all` returns every tracked scope.
    include: Option<String>,
    /// Restrict to a single scope kind: `user_ip`, `ip`, or `subnet`.
    scope: Option<String>,
    /// Case-insensitive substring match on the scope identifier (username or IP/subnet).
    q: Option<String>,
}

/// Split a raw limiter key into its scope kind and human-readable identifier.
fn scope_and_identifier(key: &str) -> (&'static str, String) {
    if let Some(rest) = key.strip_prefix("u:") {
        ("user_ip", rest.to_string())
    } else if let Some(rest) = key.strip_prefix("i:") {
        ("ip", rest.to_string())
    } else if let Some(rest) = key.strip_prefix("s:") {
        ("subnet", rest.to_string())
    } else {
        ("unknown", key.to_string())
    }
}

#[utoipa::path(
    get,
    path = "/api/v0/meta/login-rate-limit",
    tag = "meta",
    security(("bearer_auth" = [])),
    params(
        ("include" = Option<String>, Query, description = "`locked` (default) or `all`"),
        ("scope" = Option<String>, Query, description = "Filter by scope kind: `user_ip`, `ip`, or `subnet`"),
        ("q" = Option<String>, Query, description = "Case-insensitive substring match on the scope identifier")
    ),
    responses(
        (status = 200, description = "Login rate-limit configuration and tracked scopes", body = LoginRateLimitStateResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 503, description = "Shared limiter unavailable", body = ApiErrorResponse)
    )
)]
#[get("login-rate-limit")]
pub async fn get_login_rate_limit_state(
    requestor: AdminAccess,
    query: web::Query<LoginRateLimitQuery>,
) -> Result<impl Responder, ApiError> {
    let cfg = login_rate_limit_config();
    let snapshots = rate_limit::snapshot().await?;
    let tracked_entries = snapshots.len();
    let locked_entries = snapshots.iter().filter(|entry| entry.locked).count();

    let include_all = query.include.as_deref() == Some("all");
    let scope_filter = query.scope.as_deref();
    let needle = query.q.as_deref().map(str::to_ascii_lowercase);

    let mut entries: Vec<LoginRateLimitEntryResponse> = snapshots
        .into_iter()
        .map(|entry| {
            let (scope, identifier) = scope_and_identifier(&entry.key);
            LoginRateLimitEntryResponse {
                id: URL_SAFE_NO_PAD.encode(&entry.key),
                scope: scope.to_string(),
                identifier,
                attempts: entry.attempts,
                locked: entry.locked,
                locked_for_seconds: entry.locked_for.map(|remaining| remaining.as_secs()),
                lockout_level: entry.lockout_level,
            }
        })
        .filter(|entry| {
            (include_all || entry.locked)
                && scope_filter.is_none_or(|scope| entry.scope == scope)
                && needle
                    .as_deref()
                    .is_none_or(|needle| entry.identifier.to_ascii_lowercase().contains(needle))
        })
        .collect();

    // Locked scopes first (the actionable ones), then by identifier for stable output.
    entries.sort_by(|a, b| {
        b.locked
            .cmp(&a.locked)
            .then_with(|| a.identifier.cmp(&b.identifier))
    });

    debug!(
        message = "Login rate-limit state requested",
        requestor = requestor.user.id
    );

    let response = LoginRateLimitStateResponse {
        config: LoginRateLimitConfigResponse {
            enabled: cfg.enabled,
            backend: get_config()?.login_rate_limit_backend.as_str().to_string(),
            max_attempts: cfg.max_attempts,
            max_attempts_per_ip: cfg.max_attempts_per_ip,
            max_attempts_per_subnet: cfg.max_attempts_per_subnet,
            window_seconds: cfg.window_seconds,
            backoff_base_seconds: cfg.backoff_base_seconds,
            backoff_max_seconds: cfg.backoff_max_seconds,
            subnet_prefix_v4: cfg.subnet_prefix_v4,
            subnet_prefix_v6: cfg.subnet_prefix_v6,
        },
        tracked_entries,
        locked_entries,
        returned_entries: entries.len(),
        entries,
    };

    Ok(ApiResponse::new(response, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v0/meta/login-rate-limit/{id}",
    tag = "meta",
    security(("bearer_auth" = [])),
    params(
        ("id" = String, Path, description = "Opaque entry id from the list endpoint")
    ),
    responses(
        (status = 200, description = "Entry released", body = ReleaseRateLimitResponse),
        (status = 400, description = "Invalid entry id", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Entry not found", body = ApiErrorResponse),
        (status = 503, description = "Shared limiter unavailable", body = ApiErrorResponse)
    )
)]
#[delete("login-rate-limit/{id}")]
pub async fn release_login_rate_limit_entry(
    requestor: AdminAccess,
    id: web::Path<String>,
) -> Result<impl Responder, ApiError> {
    let id = id.into_inner();
    let key = URL_SAFE_NO_PAD
        .decode(id.as_bytes())
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or_else(|| ApiError::BadRequest("Invalid rate-limit entry id".to_string()))?;

    if !rate_limit::release_entry(&key).await? {
        return Err(ApiError::NotFound("Rate-limit entry not found".to_string()));
    }

    debug!(
        message = "Login rate-limit entry released",
        requestor = requestor.user.id
    );

    Ok(ApiResponse::new(
        ReleaseRateLimitResponse { released: true },
        StatusCode::OK,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v0/meta/login-rate-limit",
    tag = "meta",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "All entries cleared", body = ClearRateLimitResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 503, description = "Shared limiter unavailable", body = ApiErrorResponse)
    )
)]
#[delete("login-rate-limit")]
pub async fn clear_login_rate_limit(requestor: AdminAccess) -> Result<impl Responder, ApiError> {
    let cleared = rate_limit::clear_all().await?;

    debug!(
        message = "Login rate-limit state cleared",
        requestor = requestor.user.id,
        cleared
    );

    Ok(ApiResponse::new(
        ClearRateLimitResponse { cleared },
        StatusCode::OK,
    ))
}
