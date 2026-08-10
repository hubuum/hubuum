use actix_web::{HttpRequest, Responder, delete, get, http::StatusCode, patch, post, web};

use crate::api::etag::{RevisionedResource, revision_precondition};
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::can;
use crate::errors::ApiError;
use crate::extractors::{AccessEventContext, Authenticated};
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::models::{
    CollectionID, ComputedFieldDefinition, ComputedFieldDefinitionID, ComputedFieldDefinitionPatch,
    ComputedFieldDefinitionRequest, ComputedFieldDeleteResponse, ComputedFieldListResponse,
    ComputedFieldMutationResponse, ComputedFieldPreviewRequest, ComputedFieldPreviewResponse,
    HubuumClassID, HubuumObjectID, Permissions, PersonalComputedFieldDefinitionRequest,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::AppContext;
use crate::storage::capabilities::UserPermissions;
use crate::storage::capabilities::computed_field::{
    class_computation_state_for, create_personal_definition, create_shared_definition,
    delete_personal_definition, delete_shared_definition, get_computed_definition,
    list_personal_definitions_page, list_shared_definitions, preview_computed_definition,
    request_class_rebuild, update_personal_definition, update_shared_definition,
};
use crate::storage::capabilities::with_revision_precondition_scope;
use crate::traits::SelfAccessors;

fn require_human(requestor: &Authenticated) -> Result<i32, ApiError> {
    if requestor.principal.is_human() {
        Ok(requestor.principal.id())
    } else {
        Err(ApiError::Forbidden(
            "Service accounts cannot manage personal computed fields".to_string(),
        ))
    }
}

fn computed_field_precondition(
    request: &HttpRequest,
    definition: &ComputedFieldDefinition,
) -> Result<Option<crate::api::etag::RevisionPrecondition>, ApiError> {
    revision_precondition(request, definition)
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
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let definitions = list_shared_definitions(&context, class.id).await?;
    let state = class_computation_state_for(&context, class.id).await?;
    Ok(ApiResponse::ok(ComputedFieldListResponse {
        definitions,
        state,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("field_id" = i32, Path, description = "Computed-field definition ID")
    ),
    responses(
        (status = 200, description = "Shared computed-field definition", body = ComputedFieldDefinition),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Computed field or class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/computed-fields/{field_id}")]
pub async fn get_shared_computed_field(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, ComputedFieldDefinitionID)>,
) -> Result<impl Responder, ApiError> {
    let (class_id, field_id) = path.into_inner();
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let definition = get_computed_definition(&context, field_id.id()).await?;
    if definition.class_id != class.id || !definition.is_shared() {
        return Err(ApiError::NotFound(format!(
            "Shared computed field {} was not found in class {}",
            field_id.id(),
            class.id
        )));
    }
    ApiResponse::ok_revisioned(definition)
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
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    request: web::Json<ComputedFieldDefinitionRequest>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&context).await?;
    let collection = CollectionID::new(class.collection_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        collection
    );
    let event_context = requestor.event_context(&http_request);
    let response = create_shared_definition(
        &context,
        class.id,
        class.collection_id,
        requestor.principal.id(),
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
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, ComputedFieldDefinitionID)>,
    request: web::Json<ComputedFieldDefinitionPatch>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, field_id) = path.into_inner();
    let class = class_id.instance(&context).await?;
    let collection = CollectionID::new(class.collection_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        collection
    );
    let definition = get_computed_definition(&context, field_id.id()).await?;
    if definition.class_id != class.id || !definition.is_shared() {
        return Err(ApiError::NotFound(format!(
            "Shared computed field {} was not found in class {}",
            field_id.id(),
            class.id
        )));
    }
    let precondition = computed_field_precondition(&http_request, &definition)?;
    let event_context = requestor.event_context(&http_request);
    let response = with_revision_precondition_scope(
        precondition,
        update_shared_definition(
            &context,
            class.id,
            class.collection_id,
            field_id.id(),
            requestor.principal.id(),
            request.into_inner(),
            &event_context,
        ),
    )
    .await?;
    Ok(ApiResponse::new(response, StatusCode::OK))
}

#[utoipa::path(
    delete,
    path = "/api/v1/classes/{class_id}/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("field_id" = i32, Path, description = "Computed-field definition ID"),
    ),
    responses(
        (status = 202, description = "Shared definition deleted and rebuild queued", body = ComputedFieldDeleteResponse),
        (status = 409, description = "Revision conflict", body = ApiErrorResponse)
    )
)]
#[delete("/{class_id}/computed-fields/{field_id}")]
pub async fn delete_shared_computed_field(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, ComputedFieldDefinitionID)>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, field_id) = path.into_inner();
    let class = class_id.instance(&context).await?;
    let collection = CollectionID::new(class.collection_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        collection
    );
    let definition = get_computed_definition(&context, field_id.id()).await?;
    if definition.class_id != class.id || !definition.is_shared() {
        return Err(ApiError::NotFound(format!(
            "Shared computed field {} was not found in class {}",
            field_id.id(),
            class.id
        )));
    }
    let precondition = computed_field_precondition(&http_request, &definition)?;
    let event_context = requestor.event_context(&http_request);
    let state = with_revision_precondition_scope(
        precondition,
        delete_shared_definition(
            &context,
            class.id,
            class.collection_id,
            field_id.id(),
            requestor.principal.id(),
            &event_context,
        ),
    )
    .await?;
    Ok(ApiResponse::new(
        ComputedFieldDeleteResponse {
            deleted_definition_id: field_id.id(),
            state,
        },
        StatusCode::ACCEPTED,
    ))
}

async fn preview_source(
    context: &AppContext,
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
    let object = object_id.instance(context).await?;
    if object.hubuum_class_id != target_class_id {
        return Err(ApiError::BadRequest(format!(
            "Object {} is not in class {target_class_id}",
            object.id
        )));
    }
    can!(
        context,
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
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    request: web::Json<ComputedFieldPreviewRequest>,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&context).await?;
    let collection = CollectionID::new(class.collection_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        collection
    );
    let request = request.into_inner();
    let data = preview_source(&context, &requestor, &request, class.id).await?;
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
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let class = class_id.instance(&context).await?;
    let collection = CollectionID::new(class.collection_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::UpdateCollection],
        collection
    );
    Ok(ApiResponse::accepted(
        request_class_rebuild(
            &context,
            class.id,
            class.collection_id,
            Some(requestor.principal.id()),
        )
        .await?,
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
    context: AppContext,
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
        list_personal_definitions_page(&context, owner_id, class_filter, &search_params).await?;
    ApiResponse::paginated(definitions, total_count, &params)
}

#[utoipa::path(
    get,
    path = "/api/v1/iam/me/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(("field_id" = i32, Path, description = "Computed-field definition ID")),
    responses(
        (status = 200, description = "Personal computed-field definition", body = ComputedFieldDefinition),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Service accounts are not allowed", body = ApiErrorResponse),
        (status = 404, description = "Computed field not found", body = ApiErrorResponse)
    )
)]
#[get("/computed-fields/{field_id}")]
pub async fn get_personal_computed_field(
    context: AppContext,
    requestor: Authenticated,
    field_id: web::Path<ComputedFieldDefinitionID>,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    let field_id = field_id.into_inner();
    let definition = get_computed_definition(&context, field_id.id()).await?;
    if !definition.is_personal_for(owner_id) {
        return Err(ApiError::NotFound(format!(
            "Personal computed field {} was not found",
            field_id.id()
        )));
    }
    let class = HubuumClassID::new(definition.class_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    ApiResponse::ok_revisioned(definition)
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
    context: AppContext,
    requestor: Authenticated,
    request: web::Json<PersonalComputedFieldDefinitionRequest>,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    let request = request.into_inner();
    let class_id = HubuumClassID::new(request.class_id)?;
    let class = class_id.instance(&context).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    ApiResponse::revisioned(
        create_personal_definition(&context, class.id, owner_id, request.definition).await?,
        StatusCode::CREATED,
    )
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
    context: AppContext,
    requestor: Authenticated,
    field_id: web::Path<ComputedFieldDefinitionID>,
    request: web::Json<ComputedFieldDefinitionPatch>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    let field_id = field_id.into_inner();
    let definition = get_computed_definition(&context, field_id.id()).await?;
    if !definition.is_personal_for(owner_id) {
        return Err(ApiError::NotFound(format!(
            "Personal computed field {} was not found",
            field_id.id()
        )));
    }
    let class = HubuumClassID::new(definition.class_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let precondition = computed_field_precondition(&http_request, &definition)?;
    let updated = with_revision_precondition_scope(
        precondition,
        update_personal_definition(&context, owner_id, field_id.id(), request.into_inner()),
    )
    .await?;
    ApiResponse::ok_revisioned(updated)
}

#[utoipa::path(
    delete,
    path = "/api/v1/iam/me/computed-fields/{field_id}",
    tag = "computed fields",
    security(("bearer_auth" = [])),
    params(
        ("field_id" = i32, Path, description = "Computed-field definition ID"),
    ),
    responses(
        (status = 204, description = "Personal computed field deleted"),
        (status = 409, description = "Revision conflict", body = ApiErrorResponse)
    )
)]
#[delete("/computed-fields/{field_id}")]
pub async fn delete_personal_computed_field(
    context: AppContext,
    requestor: Authenticated,
    field_id: web::Path<ComputedFieldDefinitionID>,
    http_request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let owner_id = require_human(&requestor)?;
    let field_id = field_id.into_inner();
    let definition = get_computed_definition(&context, field_id.id()).await?;
    if !definition.is_personal_for(owner_id) {
        return Err(ApiError::NotFound(format!(
            "Personal computed field {} was not found",
            field_id.id()
        )));
    }
    let class = HubuumClassID::new(definition.class_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let precondition = computed_field_precondition(&http_request, &definition)?;
    with_revision_precondition_scope(
        precondition,
        delete_personal_definition(&context, owner_id, field_id.id()),
    )
    .await?;
    Ok(ApiResponse::no_content_with_etag(definition.entity_tag()?))
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
    context: AppContext,
    requestor: Authenticated,
    request: web::Json<ComputedFieldPreviewRequest>,
) -> Result<impl Responder, ApiError> {
    let _ = require_human(&requestor)?;
    let request = request.into_inner();
    let target_class_id = request.class_id.ok_or_else(|| {
        ApiError::BadRequest("class_id is required for a personal preview".to_string())
    })?;
    let class = HubuumClassID::new(target_class_id)?
        .instance(&context)
        .await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        class
    );
    let data = preview_source(&context, &requestor, &request, target_class_id).await?;
    Ok(ApiResponse::ok(preview_computed_definition(
        &data,
        &request.definition,
    )?))
}
