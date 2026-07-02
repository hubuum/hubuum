use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use tracing::{debug, info};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::can;
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::namespace::user_can_on_any;
use crate::models::search::parse_query_parameter;
use crate::models::{
    NamespaceID, NewReportTemplate, Permissions, ReportTemplate, ReportTemplateID,
    ReportTemplateRunRequest, TaskResponse, UpdateReportTemplate,
};
use crate::pagination::prepare_db_pagination;
use crate::traits::{CanDelete, CanSave, CanUpdate, NamespaceAccessors, SelfAccessors};

crate::history_db_fns!(
    report_template_history_paginated_with_total_count,
    report_template_as_of,
    crate::schema::report_templates_history,
    crate::models::ReportTemplateHistory
);

#[utoipa::path(
    post,
    path = "/api/v1/templates",
    tag = "templates",
    security(("bearer_auth" = [])),
    request_body = NewReportTemplate,
    responses(
        (status = 201, description = "Template created", body = ReportTemplate),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[routes]
#[post("")]
#[post("/")]
pub async fn create_template(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template: web::Json<NewReportTemplate>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template = template.into_inner();

    debug!(
        message = "Report template create requested",
        user_id = user.id,
        namespace_id = template.namespace_id,
        template_name = template.name
    );

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::CreateTemplate],
        NamespaceID::new(template.namespace_id)?
    );

    let event_context = requestor.event_context(&req);
    let created = template.save(&pool, &event_context).await?;

    let location = api_locations::template(created.id)?;
    Ok(ApiResponse::created(created, location))
}

#[utoipa::path(
    get,
    path = "/api/v1/templates",
    tag = "templates",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Templates visible to caller", body = [ReportTemplate]),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("")]
#[get("/")]
pub async fn get_templates(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;

    info!(
        message = "Report template list requested",
        user_id = user.id
    );

    let search_params = prepare_db_pagination::<ReportTemplate>(&params)?;
    let allowed_namespace_ids =
        user_can_on_any(&pool, user, Permissions::ReadTemplate, requestor.scopes())
            .await?
            .into_iter()
            .map(|namespace| namespace.id)
            .collect::<Vec<_>>();

    let (templates, total_count) =
        ReportTemplate::list_with_total_count(&pool, &allowed_namespace_ids, &search_params)
            .await?;

    ApiResponse::paginated(templates, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/templates/{template_id}",
    tag = "templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Template ID")
    ),
    responses(
        (status = 200, description = "Template", body = ReportTemplate),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}")]
pub async fn get_template(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template_id: web::Path<ReportTemplateID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();

    debug!(
        message = "Report template get requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let template = template_id.instance(&pool).await?;

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadTemplate],
        NamespaceID::new(template.namespace_id)?
    );

    Ok(ApiResponse::new(template, StatusCode::OK))
}

#[utoipa::path(
    post,
    path = "/api/v1/templates/{template_id}/reports",
    tag = "templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Executable report template ID")
    ),
    request_body = ReportTemplateRunRequest,
    responses(
        (status = 202, description = "Report task accepted", body = TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 429, description = "Too many active report tasks", body = ApiErrorResponse)
    )
)]
#[post("/{template_id}/reports")]
pub async fn run_template_report(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    template_id: web::Path<ReportTemplateID>,
    run: web::Json<ReportTemplateRunRequest>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let run = run.into_inner();

    debug!(
        message = "Report template execution requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let template = template_id.instance(&pool).await?;

    can!(
        &pool,
        user.clone(),
        requestor.scopes(),
        [Permissions::ReadTemplate],
        NamespaceID::new(template.namespace_id)?
    );

    let report = template.build_report_request(run)?;
    let task = crate::api::v1::handlers::reports::submit_report_task(
        &pool,
        user,
        requestor.scopes(),
        Some(requestor.token_meta.id),
        req,
        report,
        Some(template),
    )
    .await?;
    let response = task.to_response()?;

    Ok(ApiResponse::accepted_at(
        response,
        api_locations::task(task.id)?,
    ))
}

#[utoipa::path(
    patch,
    path = "/api/v1/templates/{template_id}",
    tag = "templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Template ID")
    ),
    request_body = UpdateReportTemplate,
    responses(
        (status = 200, description = "Template updated", body = ReportTemplate),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse)
    )
)]
#[patch("/{template_id}")]
pub async fn patch_template(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template_id: web::Path<ReportTemplateID>,
    update: web::Json<UpdateReportTemplate>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let update = update.into_inner();

    debug!(
        message = "Report template patch requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let existing = template_id.instance(&pool).await?;

    can!(
        &pool,
        user.clone(),
        requestor.scopes(),
        [Permissions::UpdateTemplate],
        NamespaceID::new(existing.namespace_id)?
    );

    if let Some(target_namespace) = update.namespace_id
        && target_namespace != existing.namespace_id
    {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateTemplate],
            NamespaceID::new(target_namespace)?
        );
    }

    let event_context = requestor.event_context(&req);
    let updated = update.update(&pool, existing.id, &event_context).await?;

    Ok(ApiResponse::new(updated, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/templates/{template_id}",
    tag = "templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Template ID")
    ),
    responses(
        (status = 204, description = "Template deleted"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse)
    )
)]
#[delete("/{template_id}")]
pub async fn delete_template(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template_id: web::Path<ReportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();

    debug!(
        message = "Report template delete requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let template = template_id.instance(&pool).await?;

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::DeleteTemplate],
        NamespaceID::new(template.namespace_id)?
    );

    let event_context = requestor.event_context(&req);
    template_id.delete(&pool, &event_context).await?;

    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    get,
    path = "/api/v1/templates/{template_id}/history",
    tag = "templates",
    security(("bearer_auth" = [])),
    params(("template_id" = i32, Path, description = "Template ID")),
    responses(
        (status = 200, description = "Template history", body = [crate::api::v1::handlers::history::HistoryResponse<crate::models::ReportTemplateHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}/history")]
pub async fn get_template_history(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template_id: web::Path<ReportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{HistoryResponse, resolve_actor_usernames};
    use crate::models::search::parse_query_parameter;
    use crate::pagination::prepare_db_pagination;

    let user = &requestor.principal;
    let instance = template_id.into_inner().instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadTemplate],
        NamespaceID::new(instance.namespace_id)?
    );

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<crate::models::ReportTemplateHistory>(&params)?;
    let (rows, total_count) =
        report_template_history_paginated_with_total_count(instance.id, &pool, &search_params)
            .await?;

    let actor_ids = rows.iter().filter_map(|r| r.actor_id).collect();
    let actor_map = resolve_actor_usernames(&pool, actor_ids).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| {
                let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
                HistoryResponse {
                    entry: row,
                    actor_username,
                }
            })
            .collect()
    })
}

#[utoipa::path(
    get,
    path = "/api/v1/templates/{template_id}/history/as-of",
    tag = "templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Template ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Template version at timestamp", body = crate::api::v1::handlers::history::HistoryResponse<crate::models::ReportTemplateHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}/history/as-of")]
pub async fn get_template_as_of(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template_id: web::Path<ReportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, parse_as_of, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let instance = template_id.into_inner().instance(&pool).await?;
    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadTemplate],
        NamespaceID::new(instance.namespace_id)?
    );

    let at = parse_as_of(req.query_string())?;
    let row = report_template_as_of(instance.id, at, &pool)
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!("no version of template {} at {at}", instance.id))
        })?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
            entry: row,
            actor_username,
    }))
}
