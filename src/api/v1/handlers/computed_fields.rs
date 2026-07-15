use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, web};
use serde::Deserialize;
use utoipa::IntoParams;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::can;
use crate::db::traits::UserPermissions;
use crate::db::traits::computed_field::{
    class_computation_state_for, create_personal_definition, create_shared_definition,
    delete_personal_definition, delete_shared_definition, get_computed_definition,
    list_personal_definitions_page, list_shared_definitions, preview_computed_definition,
    request_class_rebuild, update_personal_definition, update_shared_definition,
};
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::models::{
    ComputedFieldDefinition, ComputedFieldDefinitionID, ComputedFieldDefinitionPatch,
    ComputedFieldDefinitionRequest, ComputedFieldDeleteResponse, ComputedFieldListResponse,
    ComputedFieldMutationResponse, ComputedFieldPreviewRequest, ComputedFieldPreviewResponse,
    HubuumClassID, HubuumObjectID, Permissions, PersonalComputedFieldDefinitionRequest,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::traits::SelfAccessors;

#[derive(Debug, Deserialize, IntoParams)]
pub struct ExpectedRevisionQuery {
    pub expected_revision: i64,
}

fn require_human(requestor: &Authenticated) -> Result<i32, ApiError> {
    if requestor.principal.is_human() {
        Ok(requestor.principal.id)
    } else {
        Err(ApiError::Forbidden(
            "Service accounts cannot manage personal computed fields".to_string(),
        ))
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/computed-fields",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID")),
    responses(
        (status = 200, description = "Shared computed-field definitions and rebuild state", body = ComputedFieldListResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/computed-fields")]
pub async fn get_shared_computed_fields(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let definitions = list_shared_definitions(&pool, class.id).await?;
    let state = class_computation_state_for(&pool, class.id).await?;
    Ok(ApiResponse::ok(ComputedFieldListResponse {
        definitions,
        state,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/computed-fields",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID")),
    request_body = ComputedFieldDefinitionRequest,
    responses(
        (status = 201, description = "Shared definition created and rebuild queued", body = ComputedFieldMutationResponse),
        (status = 400, description = "Invalid definition", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 409, description = "Key conflict", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/computed-fields")]
pub async fn create_shared_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    request: web::Json<ComputedFieldDefinitionRequest>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    let event_context = requestor.event_context(&http_request);
    let response = create_shared_definition(
        &pool,
        class.id,
        requestor.principal.id,
        request.into_inner(),
        &event_context,
    )
    .await?;
    Ok(ApiResponse::new(response, StatusCode::CREATED))
}

#[utoipa::path(
    patch,
    path = "/api/v1/classes/{class_id}/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("field_id" = i32, Path, description = "Computed-field definition ID")
    ),
    request_body = ComputedFieldDefinitionPatch,
    responses(
        (status = 200, description = "Shared definition updated", body = ComputedFieldMutationResponse),
        (status = 400, description = "Invalid definition", body = ApiErrorResponse),
        (status = 409, description = "Revision or key conflict", body = ApiErrorResponse)
    )
)]
#[patch("/{class_id}/computed-fields/{field_id}")]
pub async fn patch_shared_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, ComputedFieldDefinitionID)>,
    request: web::Json<ComputedFieldDefinitionPatch>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, field_id) = path.into_inner();
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    let event_context = requestor.event_context(&http_request);
    let response = update_shared_definition(
        &pool,
        class.id,
        field_id.id(),
        requestor.principal.id,
        request.into_inner(),
        &event_context,
    )
    .await?;
    Ok(ApiResponse::ok(response))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("field_id" = i32, Path, description = "Computed-field definition ID"),
        ExpectedRevisionQuery
    ),
    responses(
        (status = 202, description = "Shared definition deleted and rebuild queued", body = ComputedFieldDeleteResponse),
        (status = 409, description = "Revision conflict", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/computed-fields/{field_id}")]
pub async fn delete_shared_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, ComputedFieldDefinitionID)>,
    query: web::Query<ExpectedRevisionQuery>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, field_id) = path.into_inner();
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    let event_context = requestor.event_context(&http_request);
    let state = delete_shared_definition(
        &pool,
        class.id,
        field_id.id(),
        requestor.principal.id,
        query.expected_revision,
        &event_context,
    )
    .await?;
    Ok(ApiResponse::accepted(ComputedFieldDeleteResponse {
        deleted_definition_id: field_id.id(),
        state,
    }))
}

async fn preview_source(
    pool: &AppContext,
    requestor: &Authenticated,
    request: &ComputedFieldPreviewRequest,
    target_class_id: i32,
) -> Result<serde_json::Value, ApiError> {
    if request.source_count() != 1 {
        return Err(ApiError::BadRequest(
            "Preview requires exactly one of object_id or data".to_string(),
        ));
    }
    if let Some(data) = &request.data {
        return Ok(data.clone());
    }
    let object_id = HubuumObjectID::new(request.object_id.expect("source count checked"))?;
    let object = object_id.instance(pool).await?;
    if object.hubuum_class_id != target_class_id {
        return Err(ApiError::BadRequest(format!(
            "Object {} is not in class {target_class_id}",
            object.id
        )));
    }
    can!(
        pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadObject],
        object
    );
    Ok(object.data)
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/computed-fields/preview",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID")),
    request_body = ComputedFieldPreviewRequest,
    responses(
        (status = 200, description = "Computed-field preview", body = ComputedFieldPreviewResponse),
        (status = 400, description = "Invalid preview", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/computed-fields/preview")]
pub async fn preview_shared_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    request: web::Json<ComputedFieldPreviewRequest>,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    let request = request.into_inner();
    let data = preview_source(&pool, &requestor, &request, class.id).await?;
    Ok(ApiResponse::ok(preview_computed_definition(
        &data,
        &request.definition,
    )?))
}

#[utoipa::path(
    post,
    path = "/api/v1/classes/{class_id}/computed-fields/rebuild",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class ID")),
    responses(
        (status = 202, description = "Computed-field rebuild queued", body = crate::models::ClassComputationState),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[post("/{class_id}/computed-fields/rebuild")]
pub async fn rebuild_shared_computed_fields(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateClass],
        class
    );
    Ok(ApiResponse::accepted(
        request_class_rebuild(&pool, class.id, Some(requestor.principal.id)).await?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me/computed-fields",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(("class_id" = Option<i32>, Query, description = "Limit results to one class")),
    responses(
        (status = 200, description = "Current user's personal computed fields", body = [ComputedFieldDefinition]),
        (status = 403, description = "Service accounts are not allowed", body = ApiErrorResponse)
    )
)]
#[get("/computed-fields")]
pub async fn get_personal_computed_fields(
    pool: AppContext,
    requestor: Authenticated,
    request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    let (params, mut passthrough) =
        parse_query_parameter_with_passthrough(request.query_string(), &["class_id"])?;
    let class_filter = match passthrough.remove("class_id") {
        None => None,
        Some(values) if values.len() == 1 => Some(
            values[0]
                .parse::<i32>()
                .map_err(|_| ApiError::BadRequest("class_id must be an integer".to_string()))?,
        ),
        Some(_) => {
            return Err(ApiError::BadRequest(
                "class_id may be supplied at most once".to_string(),
            ));
        }
    };
    let search_params = prepare_db_pagination::<ComputedFieldDefinition>(&params)?;
    let (definitions, total_count) =
        list_personal_definitions_page(&pool, owner_id, class_filter, &search_params).await?;
    ApiResponse::paginated(definitions, total_count, &params)
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/me/computed-fields",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    request_body = PersonalComputedFieldDefinitionRequest,
    responses(
        (status = 201, description = "Personal computed field created", body = ComputedFieldDefinition),
        (status = 403, description = "Forbidden", body = ApiErrorResponse)
    )
)]
#[post("/computed-fields")]
pub async fn create_personal_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    request: web::Json<PersonalComputedFieldDefinitionRequest>,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    let request = request.into_inner();
    let class_id = HubuumClassID::new(request.class_id)?;
    let class = class_id.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    Ok(ApiResponse::new(
        create_personal_definition(&pool, class.id, owner_id, request.definition).await?,
        StatusCode::CREATED,
    ))
}

#[utoipa::path(
    patch,
    path = "/api/v1/iam/me/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(("field_id" = i32, Path, description = "Computed-field definition ID")),
    request_body = ComputedFieldDefinitionPatch,
    responses(
        (status = 200, description = "Personal computed field updated", body = ComputedFieldDefinition),
        (status = 409, description = "Revision conflict", body = ApiErrorResponse)
    )
)]
#[patch("/computed-fields/{field_id}")]
pub async fn patch_personal_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    field_id: web::Path<ComputedFieldDefinitionID>,
    request: web::Json<ComputedFieldDefinitionPatch>,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    let field_id = field_id.into_inner();
    let definition = get_computed_definition(&pool, field_id.id()).await?;
    if !definition.is_personal_for(owner_id) {
        return Err(ApiError::NotFound(format!(
            "Personal computed field {} was not found",
            field_id.id()
        )));
    }
    let class = HubuumClassID::new(definition.class_id)?
        .instance(&pool)
        .await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    Ok(ApiResponse::ok(
        update_personal_definition(&pool, owner_id, field_id.id(), request.into_inner()).await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/me/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(
        ("field_id" = i32, Path, description = "Computed-field definition ID"),
        ExpectedRevisionQuery
    ),
    responses(
        (status = 204, description = "Personal computed field deleted"),
        (status = 409, description = "Revision conflict", body = ApiErrorResponse)
    )
)]
#[delete("/computed-fields/{field_id}")]
pub async fn delete_personal_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    field_id: web::Path<ComputedFieldDefinitionID>,
    query: web::Query<ExpectedRevisionQuery>,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    delete_personal_definition(
        &pool,
        owner_id,
        field_id.into_inner().id(),
        query.expected_revision,
    )
    .await?;
    Ok(ApiResponse::no_content())
}

#[utoipa::path(
    post,
    path = "/api/v1/iam/me/computed-fields/preview",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    request_body = ComputedFieldPreviewRequest,
    responses(
        (status = 200, description = "Personal computed-field preview", body = ComputedFieldPreviewResponse),
        (status = 400, description = "Invalid preview", body = ApiErrorResponse)
    )
)]
#[post("/computed-fields/preview")]
pub async fn preview_personal_computed_field(
    pool: AppContext,
    requestor: Authenticated,
    request: web::Json<ComputedFieldPreviewRequest>,
) -> Result<impl Responder, ApiError> {
    let _ = require_human(&requestor)?;
    let request = request.into_inner();
    let target_class_id = request.class_id.ok_or_else(|| {
        ApiError::BadRequest("class_id is required for a personal preview".to_string())
    })?;
    let class = HubuumClassID::new(target_class_id)?.instance(&pool).await?;
    can!(
        &pool,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let data = preview_source(&pool, &requestor, &request, target_class_id).await?;
    Ok(ApiResponse::ok(preview_computed_definition(
        &data,
        &request.definition,
    )?))
}
