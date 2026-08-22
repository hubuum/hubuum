use actix_web::{HttpRequest, Responder, delete, get, patch, routes, web};

use crate::api::etag::{RevisionedResource, revision_precondition, revision_precondition_for_tag};
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::{ApiResponse, ResponseLocation};
use crate::can;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::parse_query_parameter;
use crate::models::{
    CollectionID, EventSubscription, EventSubscriptionID, NewEventSubscription, Permissions,
    UpdateEventSubscription,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::services::event_administration::{
    create_event_subscription as create_event_subscription_service,
    delete_event_subscription as delete_event_subscription_service,
    get_event_subscription as get_event_subscription_service, list_event_subscriptions,
    update_event_subscription as update_event_subscription_service,
};
use crate::storage::with_revision_precondition;
use crate::traits::UserPermissions;

#[utoipa::path(
    post,
    path = "/api/v1/collections/{collection_id}/event-subscriptions",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID")),
    request_body = NewEventSubscription,
    responses(
        (status = 201, description = "Event subscription created", body = EventSubscription),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Collection or sink not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("/{collection_id}/event-subscriptions")]
#[post("/{collection_id}/event-subscriptions/")]
pub async fn create_event_subscription(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    collection_id: web::Path<CollectionID>,
    subscription: web::Json<NewEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let collection_id = collection_id.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let event_context = requestor.event_context(&req);
    let created = create_event_subscription_service(
        &context,
        collection_id.id(),
        subscription.into_inner(),
        event_context,
    )
    .await?;
    let location = ResponseLocation::new(format!(
        "/api/v1/collections/{}/event-subscriptions/{}",
        created.collection_id, created.id
    ))?;
    ApiResponse::created_revisioned(created, location)
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/event-subscriptions",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID")),
    responses(
        (status = 200, description = "Event subscriptions", body = [EventSubscription]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{collection_id}/event-subscriptions")]
#[get("/{collection_id}/event-subscriptions/")]
pub async fn get_event_subscriptions(
    context: AppContext,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let collection_id = collection_id.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventSubscription>(&params)?;
    let (subscriptions, total_count) =
        list_event_subscriptions(&context, collection_id.id(), query_options).await?;
    ApiResponse::paginated(subscriptions, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/collections/{collection_id}/event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    responses(
        (status = 200, description = "Event subscription", body = EventSubscription),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse)
    )
)]
#[get("/{collection_id}/event-subscriptions/{subscription_id}")]
pub async fn get_event_subscription(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(CollectionID, EventSubscriptionID)>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, subscription_id) = path.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let subscription =
        get_event_subscription_service(&context, collection_id.id(), subscription_id.id()).await?;
    ApiResponse::ok_revisioned(subscription)
}

#[utoipa::path(
    patch,
    path = "/api/v1/collections/{collection_id}/event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    request_body = UpdateEventSubscription,
    responses(
        (status = 200, description = "Event subscription updated", body = EventSubscription),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[patch("/{collection_id}/event-subscriptions/{subscription_id}")]
pub async fn patch_event_subscription(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(CollectionID, EventSubscriptionID)>,
    update: web::Json<UpdateEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, subscription_id) = path.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let update = update.into_inner();
    if update.is_empty() {
        return Err(ApiError::BadRequest(
            "Event subscription update must include at least one field".to_string(),
        ));
    }
    let existing =
        get_event_subscription_service(&context, collection_id.id(), subscription_id.id()).await?;
    let precondition = revision_precondition(&req, &existing)?;
    let event_context = requestor.event_context(&req);
    let updated = with_revision_precondition(
        &context,
        precondition,
        update_event_subscription_service(
            &context,
            collection_id.id(),
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
    path = "/api/v1/collections/{collection_id}/event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("collection_id" = i32, Path, description = "Collection ID"),
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    responses(
        (status = 204, description = "Event subscription deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse)
    )
)]
#[delete("/{collection_id}/event-subscriptions/{subscription_id}")]
pub async fn delete_event_subscription(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(CollectionID, EventSubscriptionID)>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, subscription_id) = path.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let existing =
        get_event_subscription_service(&context, collection_id.id(), subscription_id.id()).await?;
    let etag = existing.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        delete_event_subscription_service(
            &context,
            collection_id.id(),
            subscription_id.id(),
            event_context,
        ),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(etag))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_event_subscription)
        .service(get_event_subscriptions)
        .service(get_event_subscription)
        .service(patch_event_subscription)
        .service(delete_event_subscription);
}
