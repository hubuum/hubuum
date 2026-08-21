use actix_web::{Responder, get, http::StatusCode, post, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::events::{
    event_delivery_worker_health, event_fanout_worker_health, kick_event_delivery_worker,
};
use crate::extractors::AdminAccess;
use crate::models::search::parse_query_parameter;
use crate::models::{
    EventDeliveryHealthResponse, EventDeliveryID, EventDeliveryResponse,
    EventDeliveryUpdateResponse,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::services::event_administration::{
    get_event_delivery as get_event_delivery_service, list_event_deliveries,
    mark_event_delivery_dead, release_event_delivery_for_retry,
};
use crate::storage::EventHealthStorage;

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
        (status = 200, description = "Event deliveries", body = [EventDeliveryResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_event_deliveries(
    context: AppContext,
    _admin: AdminAccess,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventDeliveryResponse>(&params)?;
    let (deliveries, total_count) = list_event_deliveries(&context, query_options).await?;
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
    context: AppContext,
    _admin: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let snapshot = context.backend().get_event_delivery_health().await?;
    Ok(ApiResponse::new(
        EventDeliveryHealthResponse::from_storage(
            snapshot,
            event_fanout_worker_health(),
            event_delivery_worker_health(),
        ),
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
        (status = 200, description = "Event delivery", body = EventDeliveryResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event delivery not found", body = ApiErrorResponse)
    )
)]
#[get("/{delivery_id}")]
pub async fn get_event_delivery(
    context: AppContext,
    _admin: AdminAccess,
    delivery_id: web::Path<EventDeliveryID>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new(
        get_event_delivery_service(&context, delivery_id.into_inner().id()).await?,
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
    context: AppContext,
    _admin: AdminAccess,
    delivery_id: web::Path<EventDeliveryID>,
) -> Result<impl Responder, ApiError> {
    let delivery =
        release_event_delivery_for_retry(&context, delivery_id.into_inner().id()).await?;
    kick_event_delivery_worker(context.clone());
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
    context: AppContext,
    _admin: AdminAccess,
    delivery_id: web::Path<EventDeliveryID>,
) -> Result<impl Responder, ApiError> {
    let delivery = mark_event_delivery_dead(&context, delivery_id.into_inner().id()).await?;
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
