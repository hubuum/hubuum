use actix_web::{HttpRequest, Responder, delete, get, patch, post, routes, web};
use tracing::debug;

use crate::api::etag::{RevisionedResource, revision_precondition, revision_precondition_for_tag};
use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, ManagementAccess};
use crate::models::search::parse_query_parameter;
use crate::models::{
    NewServiceAccount, ServiceAccount, ServiceAccountID, ServiceAccountPointResponse,
    ServiceAccountResponse, ServiceAccountWithName, UpdateServiceAccount,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::services::identity::{
    create_service_account as create_service_account_record,
    delete_service_account as delete_service_account_record,
    disable_service_account as disable_service_account_record, is_human_owner_group_member,
    list_manageable_service_accounts, load_service_account,
    update_service_account as update_service_account_record,
};
use crate::storage::StorageContext;
use crate::storage::with_revision_precondition;
use crate::traits::AuthzSubject;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(create_service_account)
        .service(list_service_accounts)
        .service(get_service_account)
        .service(update_service_account)
        .service(disable_service_account)
        .service(delete_service_account);
}

/// A caller may manage an SA iff they are an admin or a **human** member of the
/// SA's owner group (a service account never manages itself; see token routes).
async fn ensure_can_manage(
    context: &impl StorageContext,
    requestor: &ManagementAccess,
    sa: &ServiceAccount,
) -> Result<(), ApiError> {
    if requestor.user.is_admin(context).await?
        || is_human_owner_group_member(context, requestor.user.id, sa.owner_group_id).await?
    {
        Ok(())
    } else {
        // Avoid leaking whether a target service account exists via 403 vs 404.
        Err(ApiError::NotFound("Service account not found".to_string()))
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/service-accounts",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    request_body = NewServiceAccount,
    responses(
        (status = 201, description = "Service account created", body = ServiceAccountPointResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_service_account(
    context: AppContext,
    requestor: ManagementAccess,
    req: HttpRequest,
    new_sa: web::Json<NewServiceAccount>,
) -> Result<impl Responder, ApiError> {
    let new_sa = new_sa.into_inner();

    // Create authz: admin may create for any group; a non-admin human may create
    // only for a group they already belong to.
    if !requestor.user.is_admin(&context).await?
        && !is_human_owner_group_member(&context, requestor.user.id, new_sa.owner_group_id.id())
            .await?
    {
        return Err(ApiError::Forbidden(
            "May only create a service account owned by a group you belong to".to_string(),
        ));
    }

    debug!(
        message = "Service account create requested",
        requestor = requestor.user.id,
        name = new_sa.name.as_str(),
        owner_group_id = new_sa.owner_group_id.id()
    );

    let event_context = requestor.event_context(&req);
    let sa =
        create_service_account_record(&context, &new_sa, Some(requestor.user.id), &event_context)
            .await?;
    let response = sa.to_point_response(&context).await?;

    let location = api_locations::service_account(sa.id)?;
    ApiResponse::created_revisioned(response, location)
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/service-accounts",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Service accounts the caller may manage", body = [ServiceAccountResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn list_service_accounts(
    context: AppContext,
    requestor: ManagementAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let is_admin = requestor.user.is_admin(&context).await?;
    let params = parse_query_parameter(req.query_string())?;

    // Authorization and the optional exact count are applied by the selected
    // backend, not reconstructed as a per-row application scan.
    let search_params = prepare_db_pagination::<ServiceAccountWithName>(&params)?;
    let (accounts, total_count) =
        list_manageable_service_accounts(&context, requestor.user.id, is_admin, search_params)
            .await?;

    ApiResponse::mapped_paginated(accounts, total_count, &params, |accounts| {
        accounts
            .into_iter()
            .map(ServiceAccountResponse::from)
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/service-accounts/{service_account_id}",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    params(("service_account_id" = i32, Path, description = "Service account id")),
    responses(
        (status = 200, description = "Service account", body = ServiceAccountPointResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Not found", body = ApiErrorResponse)
    )
)]
#[get("/{service_account_id}")]
pub async fn get_service_account(
    context: AppContext,
    requestor: ManagementAccess,
    service_account_id: web::Path<ServiceAccountID>,
) -> Result<impl Responder, ApiError> {
    let sa = load_service_account(&context, service_account_id.into_inner().id()).await?;
    ensure_can_manage(&context, &requestor, &sa).await?;
    ApiResponse::ok_revisioned(sa.to_point_response(&context).await?)
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/service-accounts/{service_account_id}",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    params(("service_account_id" = i32, Path, description = "Service account id")),
    request_body = UpdateServiceAccount,
    responses(
        (status = 200, description = "Updated service account", body = ServiceAccountPointResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Not found", body = ApiErrorResponse)
    )
)]
#[patch("/{service_account_id}")]
pub async fn update_service_account(
    context: AppContext,
    requestor: ManagementAccess,
    req: HttpRequest,
    service_account_id: web::Path<ServiceAccountID>,
    update: web::Json<UpdateServiceAccount>,
) -> Result<impl Responder, ApiError> {
    let id = service_account_id.into_inner();
    let sa = load_service_account(&context, id.id()).await?;
    ensure_can_manage(&context, &requestor, &sa).await?;

    let update = update.into_inner();

    // Reassigning the owner group requires authority over the TARGET group too:
    // admin, or a human member of the new group. Managing the current group alone
    // must not let a caller hand off (or strand) the SA in a group they have no
    // rights to.
    if let Some(new_group) = update.owner_group_id
        && new_group != sa.owner_group_id
        && !requestor.user.is_admin(&context).await?
        && !is_human_owner_group_member(&context, requestor.user.id, new_group).await?
    {
        return Err(ApiError::Forbidden(
            "May only reassign a service account to a group you belong to".to_string(),
        ));
    }

    let current = sa.to_point_response(&context).await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let updated = with_revision_precondition(
        &context,
        precondition,
        update_service_account_record(&context, id.id(), &update, &event_context),
    )
    .await?;
    ApiResponse::ok_revisioned(updated.to_point_response(&context).await?)
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/service-accounts/{service_account_id}/disable",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    params(("service_account_id" = i32, Path, description = "Service account id")),
    responses(
        (status = 200, description = "Service account disabled", body = ServiceAccountPointResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Not found", body = ApiErrorResponse)
    )
)]
#[post("/{service_account_id}/disable")]
pub async fn disable_service_account(
    context: AppContext,
    requestor: ManagementAccess,
    req: HttpRequest,
    service_account_id: web::Path<ServiceAccountID>,
) -> Result<impl Responder, ApiError> {
    let id = service_account_id.into_inner();
    let sa = load_service_account(&context, id.id()).await?;
    ensure_can_manage(&context, &requestor, &sa).await?;

    let current = sa.to_point_response(&context).await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let disabled = with_revision_precondition(
        &context,
        precondition,
        disable_service_account_record(&context, id.id(), &event_context),
    )
    .await?;

    debug!(
        message = "Service account disabled",
        service_account = id.id(),
        requestor = requestor.user.id
    );

    ApiResponse::ok_revisioned(disabled.to_point_response(&context).await?)
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/service-accounts/{service_account_id}",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    params(("service_account_id" = i32, Path, description = "Service account id")),
    responses(
        (status = 204, description = "Service account deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Not found", body = ApiErrorResponse)
    )
)]
#[delete("/{service_account_id}")]
pub async fn delete_service_account(
    context: AppContext,
    requestor: ManagementAccess,
    req: HttpRequest,
    service_account_id: web::Path<ServiceAccountID>,
) -> Result<impl Responder, ApiError> {
    let id = service_account_id.into_inner();
    let sa = load_service_account(&context, id.id()).await?;
    ensure_can_manage(&context, &requestor, &sa).await?;
    let current = sa.to_point_response(&context).await?;
    let etag = current.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        delete_service_account_record(&context, id.id(), &event_context),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(etag))
}
