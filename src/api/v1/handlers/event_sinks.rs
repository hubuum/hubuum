use actix_web::{HttpRequest, Responder, delete, get, patch, routes, web};

use crate::api::etag::{RevisionedResource, revision_precondition, revision_precondition_for_tag};
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::{ApiResponse, ResponseLocation};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess};
use crate::models::search::parse_query_parameter;
use crate::models::{EventSink, EventSinkID, NewEventSink, UpdateEventSink};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::services::event_administration::{
    create_event_sink as create_event_sink_service, delete_event_sink as delete_event_sink_service,
    get_event_sink as get_event_sink_service, list_event_sinks,
    update_event_sink as update_event_sink_service,
};
use crate::storage::with_revision_precondition;

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
    context: AppContext,
    admin: AdminAccess,
    req: HttpRequest,
    sink: web::Json<NewEventSink>,
) -> Result<impl Responder, ApiError> {
    let event_context = admin.event_context(&req);
    let created = create_event_sink_service(&context, sink.into_inner(), event_context).await?;
    let location = ResponseLocation::new(format!("/api/v1/event-sinks/{}", created.id))?;
    ApiResponse::created_revisioned(created, location)
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
    context: AppContext,
    _admin: AdminAccess,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventSink>(&params)?;
    let (sinks, total_count) = list_event_sinks(&context, query_options).await?;
    ApiResponse::paginated(sinks, total_count, &params)
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
    context: AppContext,
    _admin: AdminAccess,
    sink_id: web::Path<EventSinkID>,
) -> Result<impl Responder, ApiError> {
    ApiResponse::ok_revisioned(get_event_sink_service(&context, sink_id.into_inner().id()).await?)
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
    context: AppContext,
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
    let existing = get_event_sink_service(&context, sink_id.id()).await?;
    let precondition = revision_precondition(&req, &existing)?;
    let event_context = admin.event_context(&req);
    let updated = with_revision_precondition(
        &context,
        precondition,
        update_event_sink_service(&context, existing.id, update, &existing, event_context),
    )
    .await?;
    ApiResponse::ok_revisioned(updated)
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
    context: AppContext,
    admin: AdminAccess,
    req: HttpRequest,
    sink_id: web::Path<EventSinkID>,
) -> Result<impl Responder, ApiError> {
    let sink_id = sink_id.into_inner();
    let existing = get_event_sink_service(&context, sink_id.id()).await?;
    let etag = existing.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = admin.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        delete_event_sink_service(&context, sink_id.id(), event_context),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(etag))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_event_sink)
        .service(get_event_sinks)
        .service(get_event_sink)
        .service(patch_event_sink)
        .service(delete_event_sink);
}
