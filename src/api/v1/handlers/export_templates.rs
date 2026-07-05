use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use tracing::{debug, info};

use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::can;
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::db::traits::history::{
    export_template_as_of, export_template_history_paginated_with_total_count,
};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::collection::user_can_on_any;
use crate::models::search::parse_query_parameter;
use crate::models::{
    CollectionID, ExportTemplate, ExportTemplateID, ExportTemplateRunRequest, NewExportTemplate,
    Permissions, TaskResponse, UpdateExportTemplate,
};
use crate::pagination::prepare_db_pagination;
use crate::tasks::idempotency_key_from_headers;
use crate::traits::{CanDelete, CanSave, CanUpdate, CollectionAccessors, SelfAccessors};

#[utoipa::path(
    post,
    path = "/api/v1/export-templates",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    request_body = NewExportTemplate,
    responses(
        (status = 201, description = "Template created", body = ExportTemplate),
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
    template: web::Json<NewExportTemplate>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template = template.into_inner();

    debug!(
        message = "Export template create requested",
        user_id = user.id,
        collection_id = template.collection_id,
        template_name = template.name
    );

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::CreateTemplate],
        CollectionID::new(template.collection_id)?
    );

    let event_context = requestor.event_context(&req);
    let created = template.save(&pool, &event_context).await?;

    let location = api_locations::template(created.id)?;
    Ok(ApiResponse::created(created, location))
}

#[utoipa::path(
    get,
    path = "/api/v1/export-templates",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    responses(
        (status = 200, description = "Templates visible to caller", body = [ExportTemplate]),
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
        message = "Export template list requested",
        user_id = user.id
    );

    let search_params = prepare_db_pagination::<ExportTemplate>(&params)?;
    let allowed_collection_ids =
        user_can_on_any(&pool, user, Permissions::ReadTemplate, requestor.scopes())
            .await?
            .into_iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>();

    let (templates, total_count) =
        ExportTemplate::list_with_total_count(&pool, &allowed_collection_ids, &search_params)
            .await?;

    ApiResponse::paginated(templates, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/export-templates/{template_id}",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Template ID")
    ),
    responses(
        (status = 200, description = "Template", body = ExportTemplate),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}")]
pub async fn get_template(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template_id: web::Path<ExportTemplateID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();

    debug!(
        message = "Export template get requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let template = template_id.instance(&pool).await?;

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::ReadTemplate],
        CollectionID::new(template.collection_id)?
    );

    Ok(ApiResponse::new(template, StatusCode::OK))
}

#[utoipa::path(
    post,
    path = "/api/v1/export-templates/{template_id}/exports",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Executable export template ID")
    ),
    request_body = ExportTemplateRunRequest,
    responses(
        (status = 202, description = "Export task accepted", body = TaskResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse),
        (status = 409, description = "Conflict", body = ApiErrorResponse),
        (status = 429, description = "Too many active export tasks", body = ApiErrorResponse)
    )
)]
#[post("/{template_id}/exports")]
pub async fn run_template_export(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    req: HttpRequest,
    template_id: web::Path<ExportTemplateID>,
    run: web::Json<ExportTemplateRunRequest>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let run = run.into_inner();

    debug!(
        message = "Export template execution requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let template = template_id.instance(&pool).await?;

    can!(
        &pool,
        user.clone(),
        requestor.scopes(),
        [Permissions::ReadTemplate],
        CollectionID::new(template.collection_id)?
    );

    let export = template.build_export_request(run)?;
    let idempotency_key = idempotency_key_from_headers(req.headers())?;
    let task = crate::exports::submit_export_task(
        &pool,
        user,
        requestor.scopes(),
        Some(requestor.token_meta.id),
        idempotency_key,
        export,
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
    path = "/api/v1/export-templates/{template_id}",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Template ID")
    ),
    request_body = UpdateExportTemplate,
    responses(
        (status = 200, description = "Template updated", body = ExportTemplate),
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
    template_id: web::Path<ExportTemplateID>,
    update: web::Json<UpdateExportTemplate>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let update = update.into_inner();

    debug!(
        message = "Export template patch requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let existing = template_id.instance(&pool).await?;

    can!(
        &pool,
        user.clone(),
        requestor.scopes(),
        [Permissions::UpdateTemplate],
        CollectionID::new(existing.collection_id)?
    );

    if let Some(target_collection) = update.collection_id
        && target_collection != existing.collection_id
    {
        can!(
            &pool,
            user,
            requestor.scopes(),
            [Permissions::CreateTemplate],
            CollectionID::new(target_collection)?
        );
    }

    let event_context = requestor.event_context(&req);
    let updated = update.update(&pool, existing.id, &event_context).await?;

    Ok(ApiResponse::new(updated, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/export-templates/{template_id}",
    tag = "export-templates",
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
    template_id: web::Path<ExportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();

    debug!(
        message = "Export template delete requested",
        user_id = user.id,
        template_id = template_id.id()
    );

    let template = template_id.instance(&pool).await?;

    can!(
        &pool,
        user,
        requestor.scopes(),
        [Permissions::DeleteTemplate],
        CollectionID::new(template.collection_id)?
    );

    let event_context = requestor.event_context(&req);
    template_id.delete(&pool, &event_context).await?;

    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    get,
    path = "/api/v1/export-templates/{template_id}/history",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    params(("template_id" = i32, Path, description = "Template ID")),
    responses(
        (status = 200, description = "Template history", body = [crate::api::v1::handlers::history::HistoryResponse<crate::models::ExportTemplateHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}/history")]
pub async fn get_template_history(
    pool: web::Data<DbPool>,
    requestor: Authenticated,
    template_id: web::Path<ExportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, resolve_actor_usernames,
    };
    use crate::models::search::parse_query_parameter;
    use crate::pagination::prepare_db_pagination;

    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let (entity_id, require_history) = match template_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadTemplate],
                CollectionID::new(instance.collection_id)?
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            (template_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<crate::models::ExportTemplateHistory>(&params)?;
    let (rows, total_count) =
        export_template_history_paginated_with_total_count(entity_id, &pool, &search_params)
            .await?;
    if require_history && rows.is_empty() && params.cursor.is_none() {
        return Err(ApiError::NotFound(format!(
            "template {entity_id} not found"
        )));
    }

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
    path = "/api/v1/export-templates/{template_id}/history/as-of",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    params(
        ("template_id" = i32, Path, description = "Template ID"),
        ("at" = String, Query, description = "RFC3339 timestamp")
    ),
    responses(
        (status = 200, description = "Template version at timestamp", body = crate::api::v1::handlers::history::HistoryResponse<crate::models::ExportTemplateHistory>),
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
    template_id: web::Path<ExportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, can_read_deleted_history, parse_as_of, resolve_actor_usernames,
    };

    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let entity_id = match template_id.instance(&pool).await {
        Ok(instance) => {
            can!(
                &pool,
                user,
                requestor.scopes(),
                [Permissions::ReadTemplate],
                CollectionID::new(instance.collection_id)?
            );
            instance.id
        }
        Err(ApiError::NotFound(_)) if can_read_deleted_history(&pool, &requestor).await? => {
            template_id.id()
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = export_template_as_of(entity_id, at, &pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no version of template {entity_id} at {at}")))?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(ApiResponse::ok(HistoryResponse {
        entry: row,
        actor_username,
    }))
}
