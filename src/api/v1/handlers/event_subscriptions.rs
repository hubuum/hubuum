use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::{ApiResponse, ResponseLocation};
use crate::can;
use crate::db::traits::UserPermissions;
use crate::db::traits::event_subscription::{
    DeleteEventSubscriptionRecord, SaveEventSubscriptionRecord, UpdateEventSubscriptionRecord,
};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::parse_query_parameter;
use crate::models::{
    CollectionID, EventSubscription, EventSubscriptionID, NewEventSubscription, Permissions,
    UpdateEventSubscription,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;

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
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    collection_id: web::Path<CollectionID>,
    subscription: web::Json<NewEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let collection_id = collection_id.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    subscription.sink_id.instance(&pool).await?;
    let event_context = requestor.event_context(&req);
    let created: EventSubscription = subscription
        .into_inner()
        .into_row(collection_id)?
        .save_event_subscription_record(&pool, &event_context)
        .await?
        .try_into()?;
    let location = ResponseLocation::new(format!(
        "/api/v1/collections/{}/event-subscriptions/{}",
        created.collection_id, created.id
    ))?;
    Ok(ApiResponse::created(created, location))
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
    pool: AppContext,
    requestor: Authenticated,
    collection_id: web::Path<CollectionID>,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let collection_id = collection_id.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventSubscription>(&params)?;
    let (subscriptions, total_count) =
        EventSubscription::list_with_total_count(&pool, collection_id.id(), &query_options).await?;
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
    pool: AppContext,
    requestor: Authenticated,
    path: web::Path<(CollectionID, EventSubscriptionID)>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, subscription_id) = path.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let subscription = subscription_id.instance(&pool).await?;
    ensure_subscription_collection(&subscription, collection_id)?;
    Ok(ApiResponse::new(subscription, StatusCode::OK))
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
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(CollectionID, EventSubscriptionID)>,
    update: web::Json<UpdateEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, subscription_id) = path.into_inner();
    can!(
        &pool,
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
    if let Some(sink_id) = update.sink_id {
        sink_id.instance(&pool).await?;
    }
    let existing = subscription_id.instance(&pool).await?;
    ensure_subscription_collection(&existing, collection_id)?;
    let event_context = requestor.event_context(&req);
    let updated: EventSubscription = update
        .into_row(&existing)?
        .update_event_subscription_record(&pool, existing.id, &event_context)
        .await?
        .try_into()?;
    Ok(ApiResponse::new(updated, StatusCode::OK))
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
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(CollectionID, EventSubscriptionID)>,
) -> Result<impl Responder, ApiError> {
    let (collection_id, subscription_id) = path.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection_id
    );
    let existing = subscription_id.instance(&pool).await?;
    ensure_subscription_collection(&existing, collection_id)?;
    let event_context = requestor.event_context(&req);
    subscription_id
        .delete_event_subscription_record(&pool, &event_context)
        .await?;
    Ok(ApiResponse::no_content())
}

fn ensure_subscription_collection(
    subscription: &EventSubscription,
    collection_id: CollectionID,
) -> Result<(), ApiError> {
    if subscription.collection_id == collection_id.id() {
        Ok(())
    } else {
        Err(ApiError::NotFound(
            "Event subscription not found in collection".to_string(),
        ))
    }
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_event_subscription)
        .service(get_event_subscriptions)
        .service(get_event_subscription)
        .service(patch_event_subscription)
        .service(delete_event_subscription);
}
