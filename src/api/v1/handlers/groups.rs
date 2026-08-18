use crate::api::etag::{
    IfMatchCondition, RevisionedResource, revision_precondition, revision_precondition_for_tag,
};
use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, AdminAccess, UserAccess};
use crate::models::group::{GroupID, NewGroup, UpdateGroup};
use crate::models::search::parse_query_parameter;
use crate::models::{
    GroupPointResponse, GroupResponse, Principal, PrincipalID, PrincipalMemberResponse,
};
use crate::pagination::{count_query_options, prepare_db_pagination};
use crate::permissions::AppContext;
use crate::services::groups::list as list_groups;
use crate::services::identity::load_principal_group;
use crate::storage::with_revision_precondition;
use crate::traits::{GroupIdApplicationExt, PrincipalIdApplicationExt};
use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Serialize, Deserialize)]
struct GroupMember {
    pub principal_id: PrincipalID,
    pub group_id: GroupID,
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups",
    tag = "groups",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Groups matching optional query filters", body = [GroupResponse]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_groups(
    context: AppContext,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(
        message = "Group list requested",
        requestor = requestor.user.id,
        params = ?params
    );

    let (groups, total_count) = list_groups(&context, &params).await?;
    let result = GroupResponse::from_groups(&context, groups).await?;

    ApiResponse::paginated(result, total_count, &params)
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/groups",
    tag = "groups",
    security(("bearer_auth" = [])),
    request_body = NewGroup,
    responses(
        (status = 201, description = "Group created", body = GroupPointResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_group(
    context: AppContext,
    new_group: web::Json<NewGroup>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Group create requested",
        requestor = requestor.user.id,
        new_group = ?new_group
    );

    let event_context = requestor.event_context(&req);
    let group = new_group.save(&context, &event_context).await?;

    let location = api_locations::group(group.id)?;
    ApiResponse::created_revisioned(group.to_point_response(&context).await?, location)
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups/{group_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 200, description = "Group", body = GroupPointResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[get("/{group_id}")]
pub async fn get_group(
    context: AppContext,
    group_id: web::Path<GroupID>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    let group = group_id.group(&context).await?;

    debug!(
        message = "Group get requested",
        target = group.id,
        requestor = requestor.user.id
    );

    ApiResponse::ok_revisioned(group.to_point_response(&context).await?)
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/groups/{group_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    request_body = UpdateGroup,
    responses(
        (status = 200, description = "Updated group", body = GroupPointResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Directory-managed group is read-only", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[patch("/{group_id}")]
pub async fn update_group(
    context: AppContext,
    group_id: web::Path<GroupID>,
    updated_group: web::Json<UpdateGroup>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let group_id = group_id.into_inner();
    let target_id = group_id.id();

    debug!(
        message = "Group patch requested",
        target = target_id,
        requestor = requestor.user.id
    );

    let current = group_id.group(&context).await?;
    let precondition = revision_precondition(&req, &current)?;
    let event_context = requestor.event_context(&req);
    let updated = with_revision_precondition(
        &context,
        precondition,
        updated_group
            .into_inner()
            .save(group_id, &context, &event_context),
    )
    .await?;
    ApiResponse::ok_revisioned(updated.to_point_response(&context).await?)
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/groups/{group_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 204, description = "Group deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Directory-managed group is read-only", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse),
        (status = 409, description = "Group still owns service accounts", body = ApiErrorResponse)
    )
)]
#[delete("/{group_id}")]
pub async fn delete_group(
    context: AppContext,
    group_id: web::Path<GroupID>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "Group delete requested",
        target = group_id.id(),
        requestor = requestor.user.id
    );

    let group = group_id.group(&context).await?;
    let etag = group.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        group_id.delete(&context, &event_context),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(etag))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups/{group_id}/members",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID")
    ),
    responses(
        (status = 200, description = "Members of group", body = [PrincipalMemberResponse]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Group not found", body = ApiErrorResponse)
    )
)]
#[get("/{group_id}/members")]
pub async fn get_group_members(
    context: AppContext,
    group_id: web::Path<GroupID>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_query_parameter(req.query_string())?;

    let group = group_id.group(&context).await?;

    debug!(
        message = "Group members requested",
        target = group.id,
        requestor = requestor.user.id
    );

    let total_count = if params.include_total() {
        let count_params = count_query_options(&params);
        group
            .count_members_paginated(&context, &count_params)
            .await?
    } else {
        crate::pagination::SKIPPED_TOTAL_COUNT
    };
    let search_params = prepare_db_pagination::<Principal>(&params)?;
    let members = group.members_paginated(&context, &search_params).await?;

    let response = PrincipalMemberResponse::from_memberships(&context, members).await?;
    ApiResponse::paginated(response, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 200, description = "Membership", body = PrincipalMemberResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Membership not found", body = ApiErrorResponse)
    )
)]
#[get("/{group_id}/members/{principal_id}")]
pub async fn get_group_member(
    context: AppContext,
    user_group_ids: web::Path<GroupMember>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group_id.group(&context).await?;
    let principal = user_group_ids.principal_id.principal(&context).await?;
    let membership = load_principal_group(&context, principal.id, group.id).await?;

    debug!(
        message = "Group membership requested",
        principal = principal.id,
        group = group.id,
        requestor = requestor.user.id
    );

    let response = PrincipalMemberResponse::point(membership);
    ApiResponse::ok_revisioned(response)
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 201, description = "Membership created", body = PrincipalMemberResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User or group not found", body = ApiErrorResponse)
    )
)]
#[post("/{group_id}/members/{principal_id}")]
pub async fn add_group_member(
    context: AppContext,
    user_group_ids: web::Path<GroupMember>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group_id.group(&context).await?;
    group.ensure_local_writes_allowed()?;
    let principal = user_group_ids.principal_id.principal(&context).await?;

    debug!(
        message = "Adding principal to group",
        principal = principal.id,
        group = group.id,
        requestor = requestor.user.id
    );

    let condition = IfMatchCondition::from_request(&req)?;
    let current = load_principal_group(&context, principal.id, group.id).await;
    let precondition = match current {
        Ok(current) => condition.database_precondition(&current.entity_tag()?)?,
        Err(ApiError::NotFound(_)) if matches!(condition, IfMatchCondition::Missing) => None,
        Err(ApiError::NotFound(_)) => {
            return Err(ApiError::PreconditionFailed(
                "The membership does not exist; refetch it before retrying".to_string(),
                None,
            ));
        }
        Err(error) => return Err(error),
    };

    let event_context = requestor.event_context(&req);
    let membership = with_revision_precondition(
        &context,
        precondition,
        group.add_member(&context, &principal, &event_context),
    )
    .await?;
    let response = PrincipalMemberResponse::point(membership);
    ApiResponse::revisioned(response, StatusCode::CREATED)
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    tag = "groups",
    security(("bearer_auth" = [])),
    params(
        ("group_id" = i32, Path, description = "Group ID"),
        ("principal_id" = i32, Path, description = "Principal ID")
    ),
    responses(
        (status = 204, description = "User removed from group"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "User or group not found", body = ApiErrorResponse)
    )
)]
#[delete("/{group_id}/members/{principal_id}")]
pub async fn delete_group_member(
    context: AppContext,
    user_group_ids: web::Path<GroupMember>,
    requestor: AdminAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let group = user_group_ids.group_id.group(&context).await?;
    group.ensure_local_writes_allowed()?;
    let principal = user_group_ids.principal_id.principal(&context).await?;

    debug!(
        message = "Deleting principal from group",
        principal = principal.id,
        group = group.id,
        requestor = requestor.user.id
    );

    let membership = load_principal_group(&context, principal.id, group.id).await?;
    let precondition = revision_precondition(&req, &membership)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition(
        &context,
        precondition,
        group.remove_member(&principal, &context, &event_context),
    )
    .await?;
    match load_principal_group(&context, principal.id, group.id).await {
        Ok(surviving) => Ok(ApiResponse::no_content_with_etag(surviving.entity_tag()?)),
        Err(ApiError::NotFound(_)) => Ok(ApiResponse::no_content()),
        Err(error) => Err(error),
    }
}
