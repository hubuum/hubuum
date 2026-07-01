use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use serde_json::json;
use tracing::debug;

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, ManagementAccess};
use crate::models::principal::load_principal_by_id;
use crate::models::search::parse_query_parameter;
use crate::models::service_account::{
    cancel_pending_tasks_for_principal, count_manageable_service_accounts,
    is_human_owner_group_member, revoke_all_tokens_for_principal,
    search_manageable_service_accounts,
};
use crate::models::{
    NewServiceAccount, ServiceAccount, ServiceAccountID, ServiceAccountResponse,
    ServiceAccountWithName, UpdateServiceAccount,
};
use crate::pagination::{count_query_options, prepare_db_pagination};
use crate::traits::AuthzSubject;
use crate::utilities::response::{
    json_response, json_response_created, paginated_json_mapped_response,
};

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
    pool: &DbPool,
    requestor: &ManagementAccess,
    sa: &ServiceAccount,
) -> Result<(), ApiError> {
    if requestor.user.is_admin(pool).await?
        || is_human_owner_group_member(pool, requestor.user.id, sa.owner_group_id).await?
    {
        Ok(())
    } else {
        // Avoid leaking whether a target service account exists via 403 vs 404.
        Err(ApiError::NotFound("Service account not found".to_string()))
    }
}

async fn response_for(
    pool: &DbPool,
    sa: &ServiceAccount,
) -> Result<ServiceAccountResponse, ApiError> {
    let name = load_principal_by_id(pool, sa.id).await?.name;
    Ok(ServiceAccountResponse::from_parts(sa, name))
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/service-accounts",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    request_body = NewServiceAccount,
    responses(
        (status = 201, description = "Service account created", body = ServiceAccountResponse),
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
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    req: HttpRequest,
    new_sa: web::Json<NewServiceAccount>,
) -> Result<impl Responder, ApiError> {
    let new_sa = new_sa.into_inner();

    // Create authz: admin may create for any group; a non-admin human may create
    // only for a group they already belong to.
    if !requestor.user.is_admin(&pool).await?
        && !is_human_owner_group_member(&pool, requestor.user.id, new_sa.owner_group_id).await?
    {
        return Err(ApiError::Forbidden(
            "May only create a service account owned by a group you belong to".to_string(),
        ));
    }

    debug!(
        message = "Service account create requested",
        requestor = requestor.user.id,
        name = new_sa.name.as_str(),
        owner_group_id = new_sa.owner_group_id
    );

    let event_context = requestor.event_context(&req);
    let sa = new_sa
        .save(&pool, Some(requestor.user.id), Some(&event_context))
        .await?;
    let response = response_for(&pool, &sa).await?;

    Ok(json_response_created(
        response,
        format!("/api/v1/iam/service-accounts/{}", sa.id).as_str(),
    ))
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
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let is_admin = requestor.user.is_admin(&pool).await?;
    let params = parse_query_parameter(req.query_string())?;

    // Authorization is applied in SQL (admin sees all; otherwise owner-group
    // membership), so this is a single paginated query, not a per-row scan.
    let total_count = count_manageable_service_accounts(
        &pool,
        &requestor.user,
        is_admin,
        count_query_options(&params),
    )
    .await?;
    let search_params = prepare_db_pagination::<ServiceAccountWithName>(&params)?;
    let accounts =
        search_manageable_service_accounts(&pool, &requestor.user, is_admin, search_params).await?;

    paginated_json_mapped_response(accounts, total_count, StatusCode::OK, &params, |accounts| {
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
        (status = 200, description = "Service account", body = ServiceAccountResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Not found", body = ApiErrorResponse)
    )
)]
#[get("/{service_account_id}")]
pub async fn get_service_account(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    service_account_id: web::Path<ServiceAccountID>,
) -> Result<impl Responder, ApiError> {
    let sa = service_account_id
        .into_inner()
        .service_account(&pool)
        .await?;
    ensure_can_manage(&pool, &requestor, &sa).await?;
    Ok(json_response(
        response_for(&pool, &sa).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/service-accounts/{service_account_id}",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    params(("service_account_id" = i32, Path, description = "Service account id")),
    request_body = UpdateServiceAccount,
    responses(
        (status = 200, description = "Updated service account", body = ServiceAccountResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Not found", body = ApiErrorResponse)
    )
)]
#[patch("/{service_account_id}")]
pub async fn update_service_account(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    req: HttpRequest,
    service_account_id: web::Path<ServiceAccountID>,
    update: web::Json<UpdateServiceAccount>,
) -> Result<impl Responder, ApiError> {
    let id = service_account_id.into_inner();
    let sa = id.service_account(&pool).await?;
    ensure_can_manage(&pool, &requestor, &sa).await?;

    let update = update.into_inner();

    // Reassigning the owner group requires authority over the TARGET group too:
    // admin, or a human member of the new group. Managing the current group alone
    // must not let a caller hand off (or strand) the SA in a group they have no
    // rights to.
    if let Some(new_group) = update.owner_group_id
        && new_group != sa.owner_group_id
        && !requestor.user.is_admin(&pool).await?
        && !is_human_owner_group_member(&pool, requestor.user.id, new_group).await?
    {
        return Err(ApiError::Forbidden(
            "May only reassign a service account to a group you belong to".to_string(),
        ));
    }

    let event_context = requestor.event_context(&req);
    let updated = update.save(id.id(), &pool, Some(&event_context)).await?;
    Ok(json_response(
        response_for(&pool, &updated).await?,
        StatusCode::OK,
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/service-accounts/{service_account_id}/disable",
    tag = "service-accounts",
    security(("bearer_auth" = [])),
    params(("service_account_id" = i32, Path, description = "Service account id")),
    responses(
        (status = 200, description = "Service account disabled", body = ServiceAccountResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Not found", body = ApiErrorResponse)
    )
)]
#[post("/{service_account_id}/disable")]
pub async fn disable_service_account(
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    req: HttpRequest,
    service_account_id: web::Path<ServiceAccountID>,
) -> Result<impl Responder, ApiError> {
    let id = service_account_id.into_inner();
    let sa = id.service_account(&pool).await?;
    ensure_can_manage(&pool, &requestor, &sa).await?;

    let event_context = requestor.event_context(&req);
    let disabled = id.disable_with_context(&pool, Some(&event_context)).await?;
    // Immediately soft-revoke its tokens and cancel its pending work.
    revoke_all_tokens_for_principal(&pool, id.id()).await?;
    cancel_pending_tasks_for_principal(&pool, id.id()).await?;

    debug!(
        message = "Service account disabled",
        service_account = id.id(),
        requestor = requestor.user.id
    );

    Ok(json_response(
        response_for(&pool, &disabled).await?,
        StatusCode::OK,
    ))
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
    pool: web::Data<DbPool>,
    requestor: ManagementAccess,
    req: HttpRequest,
    service_account_id: web::Path<ServiceAccountID>,
) -> Result<impl Responder, ApiError> {
    let id = service_account_id.into_inner();
    let sa = id.service_account(&pool).await?;
    ensure_can_manage(&pool, &requestor, &sa).await?;
    let event_context = requestor.event_context(&req);
    id.delete(&pool, Some(&event_context)).await?;
    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}
