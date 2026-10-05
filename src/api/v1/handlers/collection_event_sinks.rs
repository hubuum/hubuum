use actix_web::{HttpRequest, Responder, delete, get, patch, post, web};
use hubuum_domain::CollectionId;

use crate::api::etag::{RevisionedResource, revision_precondition};
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::{ApiResponse, ResponseLocation};
use crate::can;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::parse_query_parameter;
use crate::models::{
    CollectionEventSink, CollectionID, EventSink, EventSinkID, NewEventSink, Permissions,
    UpdateEventSink,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::services::event_administration::{
    create_collection_event_sink, delete_event_sink, get_collection_owned_sink,
    list_collection_event_sinks, update_event_sink,
};
use crate::storage::with_revision_precondition;
use crate::traits::UserPermissions;

#[utoipa::path(get, path = "/api/v1/collections/{collection_id}/event-sinks", tag = "event-sinks", security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID")),
    responses((status = 200, body = [CollectionEventSink], description = "Permitted destinations without credentials"), (status = 403, body = ApiErrorResponse, description = "Forbidden")))]
#[get("/{collection_id}/event-sinks")]
pub async fn list(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    collection: web::Path<CollectionID>,
) -> Result<impl Responder, ApiError> {
    let collection = collection.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection
    );
    let params = parse_query_parameter(req.query_string())?;
    let options = prepare_db_pagination::<EventSink>(&params)?;
    let (sinks, total) =
        list_collection_event_sinks(&context, CollectionId::new(collection.id())?, options).await?;
    ApiResponse::mapped_paginated(sinks, total, &params, |sinks| {
        sinks.into_iter().map(CollectionEventSink::from).collect()
    })
}

#[utoipa::path(get, path = "/api/v1/collections/{collection_id}/event-sinks/{sink_id}", tag = "event-sinks", security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID"), ("sink_id" = i32, Path, description = "Owned sink ID")),
    responses((status = 200, body = CollectionEventSink, description = "Collection destination without credentials"), (status = 403, body = ApiErrorResponse, description = "Forbidden"), (status = 404, body = ApiErrorResponse, description = "Owned sink not found")))]
#[get("/{collection_id}/event-sinks/{sink_id}")]
pub async fn get_owned(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(CollectionID, EventSinkID)>,
) -> Result<impl Responder, ApiError> {
    let (collection, sink_id) = path.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection
    );
    let sink =
        get_collection_owned_sink(&context, CollectionId::new(collection.id())?, sink_id).await?;
    ApiResponse::ok_revisioned(CollectionEventSink::from(sink))
}

#[utoipa::path(post, path = "/api/v1/collections/{collection_id}/event-sinks", tag = "event-sinks", security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID")), request_body = NewEventSink,
    responses((status = 201, body = CollectionEventSink, description = "Collection webhook created"), (status = 400, body = ApiErrorResponse, description = "Invalid destination"), (status = 403, body = ApiErrorResponse, description = "Forbidden")))]
#[post("/{collection_id}/event-sinks")]
pub async fn create(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    collection: web::Path<CollectionID>,
    body: web::Json<NewEventSink>,
) -> Result<impl Responder, ApiError> {
    let collection = collection.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription, Permissions::ReadAudit],
        collection
    );
    let sink = create_collection_event_sink(
        &context,
        CollectionId::new(collection.id())?,
        body.into_inner(),
        requestor.event_context(&req),
    )
    .await?;
    let location = ResponseLocation::new(format!(
        "/api/v1/collections/{}/event-sinks/{}",
        collection.id(),
        sink.id
    ))?;
    ApiResponse::created_revisioned(CollectionEventSink::from(sink), location)
}

#[utoipa::path(patch, path = "/api/v1/collections/{collection_id}/event-sinks/{sink_id}", tag = "event-sinks", security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID"), ("sink_id" = i32, Path, description = "Owned sink ID")), request_body = UpdateEventSink,
    responses((status = 200, body = CollectionEventSink, description = "Collection webhook updated"), (status = 400, body = ApiErrorResponse, description = "Invalid destination"), (status = 403, body = ApiErrorResponse, description = "Forbidden"), (status = 404, body = ApiErrorResponse, description = "Owned sink not found")))]
#[patch("/{collection_id}/event-sinks/{sink_id}")]
pub async fn update(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(CollectionID, EventSinkID)>,
    body: web::Json<UpdateEventSink>,
) -> Result<impl Responder, ApiError> {
    let (collection, sink_id) = path.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription, Permissions::ReadAudit],
        collection
    );
    let update = body.into_inner();
    if update.is_empty() {
        return Err(ApiError::BadRequest(
            "Event sink update must include at least one field".to_string(),
        ));
    }
    let sink =
        get_collection_owned_sink(&context, CollectionId::new(collection.id())?, sink_id).await?;
    let precondition = revision_precondition(&req, &sink)?;
    let updated = with_revision_precondition(
        &context,
        precondition,
        update_event_sink(
            &context,
            sink.id,
            update,
            &sink,
            requestor.event_context(&req),
        ),
    )
    .await?;
    ApiResponse::ok_revisioned(CollectionEventSink::from(updated))
}

#[utoipa::path(delete, path = "/api/v1/collections/{collection_id}/event-sinks/{sink_id}", tag = "event-sinks", security(("bearer_auth" = [])),
    params(("collection_id" = i32, Path, description = "Collection ID"), ("sink_id" = i32, Path, description = "Owned sink ID")),
    responses((status = 204, description = "Collection webhook deleted"), (status = 403, body = ApiErrorResponse, description = "Forbidden"), (status = 404, body = ApiErrorResponse, description = "Owned sink not found")))]
#[delete("/{collection_id}/event-sinks/{sink_id}")]
pub async fn remove(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
    path: web::Path<(CollectionID, EventSinkID)>,
) -> Result<impl Responder, ApiError> {
    let (collection, sink_id) = path.into_inner();
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ManageEventSubscription],
        collection
    );
    let sink =
        get_collection_owned_sink(&context, CollectionId::new(collection.id())?, sink_id).await?;
    let etag = sink.entity_tag()?;
    let precondition = revision_precondition(&req, &sink)?;
    with_revision_precondition(
        &context,
        precondition,
        delete_event_sink(&context, sink.id, requestor.event_context(&req)),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(etag))
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(list)
        .service(get_owned)
        .service(create)
        .service(update)
        .service(remove);
}
