use crate::api::openapi::{ApiErrorResponse, CountsResponse};
use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::models::class::total_class_count;
use crate::models::object::{objects_per_class_count, total_object_count};
use crate::utilities::response::json_response;
use actix_web::{get, http::StatusCode, web, Responder, ResponseError};
use diesel::sql_query;
use diesel::sql_types::{BigInt, Nullable, Timestamp};
use diesel::QueryableByName;
use diesel::RunQueryDsl;
use serde::Serialize;
use serde_json::json;
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
            .error_response()
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
) -> impl Responder {
    let total_objects = total_object_count(&pool).await;
    let total_classes = total_class_count(&pool).await;
    let objects_per_class = objects_per_class_count(&pool).await;

    debug!(
        message = "DB count requested",
        requestor = requestor.user.id,
    );

    match (total_objects, total_classes, objects_per_class) {
        (Ok(total_objects), Ok(total_classes), Ok(objects_per_class)) => Ok(json_response(
            json!({"total_objects": total_objects, "total_classes": total_classes, "objects_per_class": objects_per_class}),
            StatusCode::OK,
        )),
        (Err(e), _, _) => Err(e),
        (_, Err(e), _) => Err(e),
        (_, _, Err(e)) => Err(e),
    }
}
