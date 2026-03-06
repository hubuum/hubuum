use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, routes, web};
use tracing::{debug, info};

use crate::api::openapi::ApiErrorResponse;
use crate::can;
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::parse_query_parameter;
use crate::models::{
    NamespaceID, NewReportTemplate, Permissions, ReportTemplate, ReportTemplateID,
    UpdateReportTemplate,
};
use crate::pagination::prepare_db_pagination;
use crate::traits::NamespaceAccessors;
use crate::utilities::response::{json_response, json_response_created, paginated_json_response};

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
    requestor: UserAccess,
    template: web::Json<NewReportTemplate>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
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
        [Permissions::CreateTemplate],
        NamespaceID(template.namespace_id)
    );

    let created = crate::models::report_template::create_report_template(&pool, template).await?;

    Ok(json_response_created(
        &created,
        &format!("/api/v1/templates/{}", created.id),
    ))
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
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let params = parse_query_parameter(req.query_string())?;

    info!(
        message = "Report template list requested",
        user_id = user.id
    );

    let search_params = prepare_db_pagination::<ReportTemplate>(&params)?;

    let allowed_namespace_ids =
        crate::models::namespace::user_can_on_any(&pool, user, Permissions::ReadTemplate)
            .await?
            .into_iter()
            .map(|namespace| namespace.id)
            .collect::<Vec<_>>();

    let templates = crate::models::report_template::list_report_templates(
        &pool,
        &allowed_namespace_ids,
        &search_params,
    )
    .await?;

    paginated_json_response(templates, StatusCode::OK, &params)
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
    requestor: UserAccess,
    template_id: web::Path<ReportTemplateID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let template_id = template_id.into_inner().0;

    debug!(
        message = "Report template get requested",
        user_id = user.id,
        template_id = template_id
    );

    let template = crate::models::report_template::report_template(&pool, template_id).await?;

    can!(
        &pool,
        user,
        [Permissions::ReadTemplate],
        NamespaceID(template.namespace_id)
    );

    Ok(json_response(template, StatusCode::OK))
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
    requestor: UserAccess,
    template_id: web::Path<ReportTemplateID>,
    update: web::Json<UpdateReportTemplate>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let template_id = template_id.into_inner().0;
    let update = update.into_inner();

    debug!(
        message = "Report template patch requested",
        user_id = user.id,
        template_id = template_id
    );

    let existing = crate::models::report_template::report_template(&pool, template_id).await?;

    can!(
        &pool,
        user.clone(),
        [Permissions::UpdateTemplate],
        NamespaceID(existing.namespace_id)
    );

    if let Some(target_namespace) = update.namespace_id
        && target_namespace != existing.namespace_id
    {
        can!(
            &pool,
            user,
            [Permissions::CreateTemplate],
            NamespaceID(target_namespace)
        );
    }

    let updated =
        crate::models::report_template::update_report_template(&pool, template_id, update).await?;

    Ok(json_response(updated, StatusCode::OK))
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
    requestor: UserAccess,
    template_id: web::Path<ReportTemplateID>,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let template_id = template_id.into_inner().0;

    debug!(
        message = "Report template delete requested",
        user_id = user.id,
        template_id = template_id
    );

    let template = crate::models::report_template::report_template(&pool, template_id).await?;

    can!(
        &pool,
        user,
        [Permissions::DeleteTemplate],
        NamespaceID(template.namespace_id)
    );

    crate::models::report_template::delete_report_template(&pool, template_id).await?;

    Ok(json_response((), StatusCode::NO_CONTENT))
}
