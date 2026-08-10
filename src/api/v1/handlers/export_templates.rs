use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, routes, web};
use tracing::{debug, info};

use crate::api::etag::{RevisionedResource, revision_precondition, revision_precondition_for_tag};
use crate::api::locations as api_locations;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::history::HistoryResponse;
use crate::can;
use crate::errors::ApiError;
use crate::exports::{ExportTaskSubmission, submit_export_task};
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::collection::user_can_on_any;
use crate::models::search::parse_query_parameter;
use crate::models::{
    CollectionID, ExportTemplate, ExportTemplateHistory, ExportTemplateID,
    ExportTemplateRunRequest, HistoryAuthorizationSnapshot, NewExportTemplate, Permissions,
    TaskResponse, TokenID, UpdateExportTemplate,
};
use crate::pagination::{count_query_options, prepare_db_pagination};
use crate::permissions::visibility::authorize_cursor_page;
use crate::permissions::{
    AppContext, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef, authorize_resources,
};
use crate::storage::capabilities::UserPermissions;
use crate::storage::capabilities::authz::scope_allows;
use crate::storage::capabilities::history::{
    HistoryCollectionFilter, export_template_as_of,
    export_template_history_paginated_with_total_count,
};
use crate::storage::capabilities::with_revision_precondition_scope;
use crate::tasks::{idempotency_key_from_headers, kick_task_worker};
use crate::traits::{CanDelete, CanSave, CanUpdate, SelfAccessors};

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
    context: AppContext,
    requestor: Authenticated,
    template: web::Json<NewExportTemplate>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template = template.into_inner();

    debug!(
        message = "Export template create requested",
        user_id = user.id(),
        collection_id = template.collection_id,
        template_name = template.name
    );

    if context.permission_backend().uses_sql_permission_store() {
        can!(
            &context,
            user,
            requestor.scopes(),
            [Permissions::CreateTemplate],
            CollectionID::new(template.collection_id)?
        );
    } else {
        authorize_resources(
            context.permission_backend(),
            &context,
            user,
            requestor.scopes(),
            vec![Permissions::CreateTemplate],
            vec![ResourceRef {
                kind: ResourceKind::Template,
                id: 0,
                attrs: ResourceAttrs {
                    collection_id: Some(template.collection_id),
                    name: Some(template.name.clone()),
                    ..Default::default()
                },
            }],
        )
        .await?;
    }

    let event_context = requestor.event_context(&req);
    let created = template.save(&context, &event_context).await?;

    let location = api_locations::template(created.id)?;
    ApiResponse::created_revisioned(created, location)
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
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let params = parse_query_parameter(req.query_string())?;

    info!(
        message = "Export template list requested",
        user_id = user.id()
    );

    let (templates, total_count) = if context
        .permission_backend()
        .supports_sql_visibility_pushdown()
    {
        let search_params = prepare_db_pagination::<ExportTemplate>(&params)?;
        let mut allowed_collection_ids = user_can_on_any(
            &context,
            user,
            Permissions::ReadTemplate,
            requestor.scopes(),
        )
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();
        if let Some(scope) = requestor.scopes() {
            scope.retain_allowed_collection_ids(&mut allowed_collection_ids);
        }
        ExportTemplate::list_with_total_count(&context, &allowed_collection_ids, &search_params)
            .await?
    } else {
        if !scope_allows(requestor.scopes(), &[Permissions::ReadTemplate]) {
            return ApiResponse::paginated(Vec::new(), 0, &params);
        }
        let mut candidate_options = count_query_options(&params);
        candidate_options.include_total = false;
        let candidates = ExportTemplate::list_candidates(&context, &candidate_options).await?;
        let principal = PrincipalRef::load(&context, user).await?;
        let search_params = prepare_db_pagination::<ExportTemplate>(&params)?;
        let page = authorize_cursor_page(
            context.permission_backend(),
            &principal,
            candidates,
            requestor.scopes(),
            vec![Permissions::ReadTemplate],
            &search_params,
            |template| ResourceRef {
                kind: ResourceKind::Template,
                id: template.id,
                attrs: ResourceAttrs {
                    collection_id: Some(template.collection_id),
                    name: Some(template.name.clone()),
                    ..Default::default()
                },
            },
        )
        .await?;
        (page.rows, page.total_count)
    };

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
    context: AppContext,
    requestor: Authenticated,
    template_id: web::Path<ExportTemplateID>,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();

    debug!(
        message = "Export template get requested",
        user_id = user.id(),
        template_id = template_id.id()
    );

    let template = template_id.instance(&context).await?;

    can!(
        &context,
        user,
        requestor.scopes(),
        [Permissions::ReadTemplate],
        &template
    );

    ApiResponse::ok_revisioned(template)
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
    context: AppContext,
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
        user_id = user.id(),
        template_id = template_id.id()
    );

    let template = template_id.instance(&context).await?;

    can!(
        &context,
        user,
        requestor.scopes(),
        [Permissions::ReadTemplate],
        &template
    );

    let export = template.build_export_request(run)?;
    let idempotency_key = idempotency_key_from_headers(req.headers())?;
    let submission = ExportTaskSubmission::for_token(
        export,
        TokenID::new(requestor.token_meta.id)?,
        requestor.scopes(),
    )
    .template(template)
    .idempotency_key(idempotency_key);
    let task = submit_export_task(&context, user, submission).await?;
    kick_task_worker(context.clone());
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
    context: AppContext,
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
        user_id = user.id(),
        template_id = template_id.id()
    );

    let existing = template_id.instance(&context).await?;

    can!(
        &context,
        user.clone(),
        requestor.scopes(),
        [Permissions::UpdateTemplate],
        &existing
    );

    if let Some(target_collection) = update.collection_id
        && target_collection != existing.collection_id
    {
        can!(
            &context,
            user,
            requestor.scopes(),
            [Permissions::CreateTemplate],
            CollectionID::new(target_collection)?
        );
    }

    let precondition = revision_precondition(&req, &existing)?;
    let event_context = requestor.event_context(&req);
    let updated = with_revision_precondition_scope(
        precondition,
        update.update(&context, template_id, &event_context),
    )
    .await?;

    ApiResponse::ok_revisioned(updated)
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
    context: AppContext,
    requestor: Authenticated,
    template_id: web::Path<ExportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = &requestor.principal;
    let template_id = template_id.into_inner();

    debug!(
        message = "Export template delete requested",
        user_id = user.id(),
        template_id = template_id.id()
    );

    let template = template_id.instance(&context).await?;

    can!(
        &context,
        user,
        requestor.scopes(),
        [Permissions::DeleteTemplate],
        &template
    );

    let etag = template.entity_tag()?;
    let precondition = revision_precondition_for_tag(&req, &etag)?;
    let event_context = requestor.event_context(&req);
    with_revision_precondition_scope(precondition, template_id.delete(&context, &event_context))
        .await?;

    Ok(ApiResponse::no_content_with_etag(etag))
}

#[utoipa::path(
    get,
    path = "/api/v1/export-templates/{template_id}/history",
    tag = "export-templates",
    security(("bearer_auth" = [])),
    params(("template_id" = i32, Path, description = "Template ID")),
    responses(
        (status = 200, description = "Template history", body = [HistoryResponse<ExportTemplateHistory>]),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template not found", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}/history")]
pub async fn get_template_history(
    context: AppContext,
    requestor: Authenticated,
    template_id: web::Path<ExportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_page, can_read_deleted_history,
        history_candidate_query_options, readable_history_collection_ids,
        resolve_history_principal_names,
    };
    use crate::models::search::parse_query_parameter;
    use crate::pagination::prepare_db_pagination;

    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let (entity_id, require_history) = match template_id.instance(&context).await {
        Ok(instance) => {
            can!(
                &context,
                user,
                requestor.scopes(),
                [Permissions::ReadTemplate],
                &instance
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_))
            if can_read_deleted_history(
                &context,
                &requestor.principal,
                requestor.scopes().is_some(),
            )
            .await? =>
        {
            (template_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<ExportTemplateHistory>(&params)?;
    let (rows, total_count) = if require_history {
        export_template_history_paginated_with_total_count(
            entity_id,
            &context,
            &search_params,
            HistoryCollectionFilter::All,
        )
        .await?
    } else if context
        .permission_backend()
        .supports_sql_visibility_pushdown()
    {
        let collection_ids = readable_history_collection_ids(
            &context,
            user,
            requestor.scopes(),
            Permissions::ReadTemplate,
        )
        .await?;
        export_template_history_paginated_with_total_count(
            entity_id,
            &context,
            &search_params,
            HistoryCollectionFilter::Visible(&collection_ids),
        )
        .await?
    } else {
        let candidate_params = history_candidate_query_options(&params);
        let (candidates, _) = export_template_history_paginated_with_total_count(
            entity_id,
            &context,
            &candidate_params,
            HistoryCollectionFilter::All,
        )
        .await?;
        authorize_history_page(
            &context,
            user,
            requestor.scopes(),
            Permissions::ReadTemplate,
            candidates,
            &search_params,
            |row| HistoryAuthorizationSnapshot::from(row),
        )
        .await?
    };
    if require_history && rows.is_empty() && params.cursor.is_none() {
        return Err(ApiError::NotFound(format!(
            "template {entity_id} not found"
        )));
    }

    let principal_names = resolve_history_principal_names(&context, &rows).await?;

    ApiResponse::mapped_paginated(rows, total_count, &params, move |rows| {
        rows.into_iter()
            .map(|row| HistoryResponse::new(row, &principal_names))
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
        (status = 200, description = "Template version at timestamp", body = HistoryResponse<ExportTemplateHistory>),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Template or version not found", body = ApiErrorResponse)
    )
)]
#[get("/{template_id}/history/as-of")]
pub async fn get_template_as_of(
    context: AppContext,
    requestor: Authenticated,
    template_id: web::Path<ExportTemplateID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{
        HistoryResponse, authorize_history_snapshot, can_read_deleted_history, parse_as_of,
        resolve_history_principal_names,
    };

    let user = &requestor.principal;
    let template_id = template_id.into_inner();
    let (entity_id, deleted) = match template_id.instance(&context).await {
        Ok(instance) => {
            can!(
                &context,
                user,
                requestor.scopes(),
                [Permissions::ReadTemplate],
                &instance
            );
            (instance.id, false)
        }
        Err(ApiError::NotFound(_))
            if can_read_deleted_history(
                &context,
                &requestor.principal,
                requestor.scopes().is_some(),
            )
            .await? =>
        {
            (template_id.id(), true)
        }
        Err(err) => return Err(err),
    };

    let at = parse_as_of(req.query_string())?;
    let row = export_template_as_of(entity_id, at, &context)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no version of template {entity_id} at {at}")))?;

    if !deleted {
        authorize_history_snapshot(
            &context,
            user,
            requestor.scopes(),
            Permissions::ReadTemplate,
            HistoryAuthorizationSnapshot::from(&row),
        )
        .await?;
    }

    let principal_names =
        resolve_history_principal_names(&context, std::slice::from_ref(&row)).await?;
    Ok(ApiResponse::new(
        HistoryResponse::new(row, &principal_names),
        StatusCode::OK,
    ))
}
