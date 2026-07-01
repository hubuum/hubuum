use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::db::traits::event_subscription::{
    DeleteEventSinkRecord, SaveEventSinkRecord, UpdateEventSinkRecord,
};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess};
use crate::models::search::parse_query_parameter;
use crate::models::{EventSink, EventSinkID, NewEventSink, UpdateEventSink};
use crate::pagination::prepare_db_pagination;
use crate::utilities::response::{json_response, json_response_created, paginated_json_response};

#[utoipa::path(
    post,
    path = "/api/v1/event-sinks",
    tag = "event-sinks",
    security(("bearer_auth" = [])),
    request_body = NewEventSink,
    responses(
        (status = 201, description = "Event sink created", body = EventSink),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_event_sink(
    pool: web::Data<DbPool>,
    admin: AdminAccess,
    req: HttpRequest,
    sink: web::Json<NewEventSink>,
) -> Result<impl Responder, ApiError> {
    let event_context = admin.event_context(&req);
    let created: EventSink = sink
        .into_inner()
        .into_row()?
        .save_event_sink_record(&pool, Some(&event_context))
        .await?
        .try_into()?;
    Ok(json_response_created(
        &created,
        &format!("/api/v1/event-sinks/{}", created.id),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/event-sinks",
    tag = "event-sinks",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Event sinks", body = [EventSink]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_event_sinks(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventSink>(&params)?;
    let (sinks, total_count) = EventSink::list_with_total_count(&pool, &query_options).await?;
    paginated_json_response(sinks, total_count, StatusCode::OK, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/event-sinks/{sink_id}",
    tag = "event-sinks",
    security(("bearer_auth" = [])),
    params(("sink_id" = i32, Path, description = "Event sink ID")),
    responses(
        (status = 200, description = "Event sink", body = EventSink),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event sink not found", body = ApiErrorResponse)
    )
)]
#[get("/{sink_id}")]
pub async fn get_event_sink(
    pool: web::Data<DbPool>,
    _admin: AdminAccess,
    sink_id: web::Path<EventSinkID>,
) -> Result<impl Responder, ApiError> {
    Ok(json_response(
        sink_id.into_inner().instance(&pool).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    patch,
    path = "/api/v1/event-sinks/{sink_id}",
    tag = "event-sinks",
    security(("bearer_auth" = [])),
    params(("sink_id" = i32, Path, description = "Event sink ID")),
    request_body = UpdateEventSink,
    responses(
        (status = 200, description = "Event sink updated", body = EventSink),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event sink not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[patch("/{sink_id}")]
pub async fn patch_event_sink(
    pool: web::Data<DbPool>,
    admin: AdminAccess,
    req: HttpRequest,
    sink_id: web::Path<EventSinkID>,
    update: web::Json<UpdateEventSink>,
) -> Result<impl Responder, ApiError> {
    let sink_id = sink_id.into_inner();
    let update = update.into_inner();
    if update.is_empty() {
        return Err(ApiError::BadRequest(
            "Event sink update must include at least one field".to_string(),
        ));
    }
    let existing = sink_id.instance(&pool).await?;
    let event_context = admin.event_context(&req);
    let updated: EventSink = update
        .into_row(&existing)?
        .update_event_sink_record(&pool, existing.id, Some(&event_context))
        .await?
        .try_into()?;
    Ok(json_response(updated, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/event-sinks/{sink_id}",
    tag = "event-sinks",
    security(("bearer_auth" = [])),
    params(("sink_id" = i32, Path, description = "Event sink ID")),
    responses(
        (status = 204, description = "Event sink deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event sink not found", body = ApiErrorResponse)
    )
)]
#[delete("/{sink_id}")]
pub async fn delete_event_sink(
    pool: web::Data<DbPool>,
    admin: AdminAccess,
    req: HttpRequest,
    sink_id: web::Path<EventSinkID>,
) -> Result<impl Responder, ApiError> {
    let event_context = admin.event_context(&req);
    sink_id
        .into_inner()
        .delete_event_sink_record(&pool, Some(&event_context))
        .await?;
    Ok(actix_web::HttpResponse::NoContent().finish())
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_event_sink)
        .service(get_event_sinks)
        .service(get_event_sink)
        .service(patch_event_sink)
        .service(delete_event_sink);
}
