use actix_web::{HttpRequest, Responder, delete, get, patch, routes, web};

use crate::api::etag::{RevisionedResource, revision_precondition, revision_precondition_for_tag};
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::{ApiResponse, ResponseLocation};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess};
use crate::models::search::parse_query_parameter;
use crate::models::{
    EventSubscription, EventSubscriptionID, NewEventSubscription, SystemEventSubscription,
    UpdateEventSubscription,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::services::event_administration::{
    create_system_event_subscription as create_system_event_subscription_service,
    delete_system_event_subscription as delete_system_event_subscription_service,
    get_system_event_subscription as get_system_event_subscription_service,
    list_system_event_subscriptions,
    update_system_event_subscription as update_system_event_subscription_service,
};
use crate::storage::with_revision_precondition;

#[utoipa::path(
    post,
    path = "/api/v1/system-event-subscriptions",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    request_body = NewEventSubscription,
    responses(
        (status = 201, description = "Event subscription created", body = SystemEventSubscription),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Collection or sink not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_system_event_subscription(
    context: AppContext,
    requestor: AdminAccess,
    req: HttpRequest,
    subscription: web::Json<NewEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let event_context = requestor.event_context(&req);
    let created = create_system_event_subscription_service(
        &context,
        subscription.into_inner(),
        event_context,
    )
    .await?;
    let location =
        ResponseLocation::new(format!("/api/v1/system-event-subscriptions/{}", created.id))?;
    ApiResponse::created_revisioned(created, location)
}

#[utoipa::path(
    get,
    path = "/api/v1/system-event-subscriptions",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Event subscriptions", body = [SystemEventSubscription]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_system_event_subscriptions(
    context: AppContext,
    _requestor: AdminAccess,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventSubscription>(&params)?;
    let (subscriptions, total_count) =
        list_system_event_subscriptions(&context, query_options).await?;
    ApiResponse::paginated(subscriptions, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/system-event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    responses(
        (status = 200, description = "Event subscription", body = SystemEventSubscription),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse)
    )
)]
#[get("/{subscription_id}")]
pub async fn get_system_event_subscription(
    context: AppContext,
    _requestor: AdminAccess,
    path: web::Path<EventSubscriptionID>,
) -> Result<impl Responder, ApiError> {
    let subscription_id = path.into_inner();
    let subscription =
        get_system_event_subscription_service(&context, subscription_id.id()).await?;
    ApiResponse::ok_revisioned(subscription)
}

#[utoipa::path(
    patch,
    path = "/api/v1/system-event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    request_body = UpdateEventSubscription,
    responses(
        (status = 200, description = "Event subscription updated", body = SystemEventSubscription),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[patch("/{subscription_id}")]
pub async fn patch_system_event_subscription(
    context: AppContext,
    requestor: AdminAccess,
    req: HttpRequest,
    path: web::Path<EventSubscriptionID>,
    update: web::Json<UpdateEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let subscription_id = path.into_inner();
    let update = update.into_inner();
    if update.is_empty() {
        return Err(ApiError::BadRequest(
            "Event subscription update must include at least one field".to_string(),
        ));
    }
    let existing = get_system_event_subscription_service(&context, subscription_id.id()).await?;
    let precondition = revision_precondition(&req, &existing)?;
    let event_context = requestor.event_context(&req);
    let updated = with_revision_precondition(
        &context,
        precondition,
        update_system_event_subscription_service(
            &context,
            existing.id,
            update,
            &existing,
            event_context,
        ),
    )
    .await?;
    ApiResponse::ok_revisioned(updated)
}

#[utoipa::path(
    delete,
    path = "/api/v1/system-event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    responses(
        (status = 204, description = "Event subscription deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse)
    )
)]
#[delete("/{subscription_id}")]
pub async fn delete_system_event_subscription(
    context: AppContext,
    requestor: AdminAccess,
    req: HttpRequest,
    path: web::Path<EventSubscriptionID>,
) -> Result<impl Responder, ApiError> {
    let subscription_id = path.into_inner();
    let existing = get_system_event_subscription_service(&context, subscription_id.id()).await?;
    let etag = existing.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        delete_system_event_subscription_service(&context, subscription_id.id(), event_context),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(etag))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_system_event_subscription)
        .service(get_system_event_subscriptions)
        .service(get_system_event_subscription)
        .service(patch_system_event_subscription)
        .service(delete_system_event_subscription);
}
