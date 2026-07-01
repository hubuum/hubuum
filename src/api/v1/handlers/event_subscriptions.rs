use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, routes, web};

use crate::api::openapi::ApiErrorResponse;
use crate::can;
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::db::traits::event_subscription::{
    DeleteEventSubscriptionRecord, SaveEventSubscriptionRecord, UpdateEventSubscriptionRecord,
};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::parse_query_parameter;
use crate::models::{
    EventSubscription, EventSubscriptionID, NamespaceID, NewEventSubscription, Permissions,
    UpdateEventSubscription,
};
use crate::pagination::prepare_db_pagination;
use crate::traits::NamespaceAccessors;
use crate::utilities::response::{json_response, json_response_created, paginated_json_response};

#[utoipa::path(
    post,
    path = "/api/v1/namespaces/{namespace_id}/event-subscriptions",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(("namespace_id" = i32, Path, description = "Namespace ID")),
    request_body = NewEventSubscription,
    responses(
        (status = 201, description = "Event subscription created", body = EventSubscription),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Namespace or sink not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("/{namespace_id}/event-subscriptions")]
#[post("/{namespace_id}/event-subscriptions/")]
pub async fn create_event_subscription(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    namespace_id: web::Path<NamespaceID>,
    subscription: web::Json<NewEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let namespace_id = namespace_id.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        namespace_id
    );
    subscription.sink_id.instance(&pool).await?;
    let event_context = requestor.event_context(&req);
    let created: EventSubscription = subscription
        .into_inner()
        .into_row(namespace_id)?
        .save_event_subscription_record(&pool, Some(&event_context))
        .await?
        .try_into()?;
    Ok(json_response_created(
        &created,
        &format!(
            "/api/v1/namespaces/{}/event-subscriptions/{}",
            created.namespace_id, created.id
        ),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/event-subscriptions",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(("namespace_id" = i32, Path, description = "Namespace ID")),
    responses(
        (status = 200, description = "Event subscriptions", body = [EventSubscription]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{namespace_id}/event-subscriptions")]
#[get("/{namespace_id}/event-subscriptions/")]
pub async fn get_event_subscriptions(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    namespace_id: web::Path<NamespaceID>,
    req: actix_web::HttpRequest,
) -> Result<impl Responder, ApiError> {
    let namespace_id = namespace_id.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        namespace_id
    );
    let params = parse_query_parameter(req.query_string())?;
    let query_options = prepare_db_pagination::<EventSubscription>(&params)?;
    let (subscriptions, total_count) =
        EventSubscription::list_with_total_count(&pool, namespace_id.id(), &query_options).await?;
    paginated_json_response(subscriptions, total_count, StatusCode::OK, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/namespaces/{namespace_id}/event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    responses(
        (status = 200, description = "Event subscription", body = EventSubscription),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse)
    )
)]
#[get("/{namespace_id}/event-subscriptions/{subscription_id}")]
pub async fn get_event_subscription(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    path: web::Path<(NamespaceID, EventSubscriptionID)>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, subscription_id) = path.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        namespace_id
    );
    let subscription = subscription_id.instance(&pool).await?;
    ensure_subscription_namespace(&subscription, namespace_id)?;
    Ok(json_response(subscription, StatusCode::OK))
}

#[utoipa::path(
    patch,
    path = "/api/v1/namespaces/{namespace_id}/event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
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
#[patch("/{namespace_id}/event-subscriptions/{subscription_id}")]
pub async fn patch_event_subscription(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(NamespaceID, EventSubscriptionID)>,
    update: web::Json<UpdateEventSubscription>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, subscription_id) = path.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        namespace_id
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
    ensure_subscription_namespace(&existing, namespace_id)?;
    let event_context = requestor.event_context(&req);
    let updated: EventSubscription = update
        .into_row(&existing)?
        .update_event_subscription_record(&pool, existing.id, Some(&event_context))
        .await?
        .try_into()?;
    Ok(json_response(updated, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/{namespace_id}/event-subscriptions/{subscription_id}",
    tag = "event-subscriptions",
    security(("bearer_auth" = [])),
    params(
        ("namespace_id" = i32, Path, description = "Namespace ID"),
        ("subscription_id" = i32, Path, description = "Event subscription ID")
    ),
    responses(
        (status = 204, description = "Event subscription deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Event subscription not found", body = ApiErrorResponse)
    )
)]
#[delete("/{namespace_id}/event-subscriptions/{subscription_id}")]
pub async fn delete_event_subscription(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(NamespaceID, EventSubscriptionID)>,
) -> Result<impl Responder, ApiError> {
    let (namespace_id, subscription_id) = path.into_inner();
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        namespace_id
    );
    let existing = subscription_id.instance(&pool).await?;
    ensure_subscription_namespace(&existing, namespace_id)?;
    let event_context = requestor.event_context(&req);
    subscription_id
        .delete_event_subscription_record(&pool, Some(&event_context))
        .await?;
    Ok(actix_web::HttpResponse::NoContent().finish())
}

fn ensure_subscription_namespace(
    subscription: &EventSubscription,
    namespace_id: NamespaceID,
) -> Result<(), ApiError> {
    if subscription.namespace_id == namespace_id.id() {
        Ok(())
    } else {
        Err(ApiError::NotFound(
            "Event subscription not found in namespace".to_string(),
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
