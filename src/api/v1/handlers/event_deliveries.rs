use actix_web::{Responder, get, http::StatusCode, post, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::db::DbPool;
use crate::db::traits::event_delivery::{
    list_event_deliveries_with_total_count, load_event_delivery, mark_event_delivery_dead,
    release_event_delivery_for_retry,
};
use crate::db::traits::event_observability::load_event_delivery_health;
use crate::errors::ApiError;
use crate::events::kick_event_delivery_worker;
use crate::extractors::AdminAccess;
use crate::models::search::parse_query_parameter;
use crate::models::{
    EventDelivery, EventDeliveryHealthResponse, EventDeliveryID, EventDeliveryUpdateResponse,
};
use crate::pagination::prepare_db_pagination;

#[utoipa::path(
    get,
    path = "/api/v1/event-deliveries",
    tag = "event-deliveries",
    security(("bearer_auth" = [])),
    params(
        ("limit" = usize, Query, description = "Cursor page size"),
        ("sort" = String, Query, description = "Comma-separated sort fields. Supported fields: id, status, created_at, updated_at, next_attempt_at"),
        ("cursor" = String, Query, description = "Cursor token from X-Next-Cursor")
    ),
    responses(
        (status = 200, description = "Event deliveries", body = [EventDelivery]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_event_deliveries(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventDelivery>(&params)?;
    let (deliveries, total_count) =
        list_event_deliveries_with_total_count(&pool, &query_options).await?;
    ApiResponse::paginated(deliveries, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/event-deliveries/health",
    tag = "event-deliveries",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Event delivery pipeline health", body = EventDeliveryHealthResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[get("/health")]
pub async fn get_event_delivery_health(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new(
        load_event_delivery_health(&pool).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/event-deliveries/{delivery_id}",
    tag = "event-deliveries",
    security(("bearer_auth" = [])),
    params(("delivery_id" = i64, Path, description = "Event delivery ID")),
    responses(
        (status = 200, description = "Event delivery", body = EventDelivery),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event delivery not found", body = ApiErrorResponse)
    )
)]
#[get("/{delivery_id}")]
pub async fn get_event_delivery(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
    delivery_id: web::Path<EventDeliveryID>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new(
        load_event_delivery(&pool, delivery_id.into_inner()).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/event-deliveries/{delivery_id}/retry",
    tag = "event-deliveries",
    security(("bearer_auth" = [])),
    params(("delivery_id" = i64, Path, description = "Event delivery ID")),
    responses(
        (status = 200, description = "Event delivery released for retry", body = EventDeliveryUpdateResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event delivery not found", body = ApiErrorResponse)
    )
)]
#[post("/{delivery_id}/retry")]
pub async fn retry_event_delivery(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
    delivery_id: web::Path<EventDeliveryID>,
) -> Result<impl Responder, ApiError> {
    let delivery = release_event_delivery_for_retry(&pool, delivery_id.into_inner()).await?;
    kick_event_delivery_worker(pool.get_ref().clone());
    Ok(ApiResponse::new(
        EventDeliveryUpdateResponse { delivery },
        StatusCode::OK,
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/event-deliveries/{delivery_id}/dead",
    tag = "event-deliveries",
    security(("bearer_auth" = [])),
    params(("delivery_id" = i64, Path, description = "Event delivery ID")),
    responses(
        (status = 200, description = "Event delivery marked dead", body = EventDeliveryUpdateResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event delivery not found", body = ApiErrorResponse)
    )
)]
#[post("/{delivery_id}/dead")]
pub async fn dead_letter_event_delivery(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
    delivery_id: web::Path<EventDeliveryID>,
) -> Result<impl Responder, ApiError> {
    let delivery = mark_event_delivery_dead(&pool, delivery_id.into_inner()).await?;
    Ok(ApiResponse::new(
        EventDeliveryUpdateResponse { delivery },
        StatusCode::OK,
    ))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(get_event_deliveries)
        .service(get_event_delivery_health)
        .service(get_event_delivery)
        .service(retry_event_delivery)
        .service(dead_letter_event_delivery);
}
