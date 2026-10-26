use super::*;
use hubuum_domain::ClassId;

use crate::models::class_fields::{ClassFieldInventory, FieldDiscoveryOptions};
use crate::services::class_fields;

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/fields",
    tag = "classes",
    summary = "Discover fields in an authorized sample of a class by ID",
    description = "Returns the same bounded inventory as name-addressed field discovery. Use the stable class ID when it is already known or the class name is a URL dot segment. Counts describe an authorized sample only; explicit truncation flags identify its limits.",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = ClassId, Path, description = "Class ID"),
        ("limit" = Option<usize>, Query, description = "Maximum authorized sample size; defaults to 100", minimum = 1, maximum = 250),
        ("depth" = Option<usize>, Query, description = "Maximum path depth; defaults to 6", minimum = 0, maximum = 32),
        ("containers" = Option<bool>, Query, description = "Include object and array paths; defaults to false")
    ),
    responses(
        (status = 200, description = "Bounded field observations", body = ClassFieldInventory),
        (status = 400, description = "Invalid discovery options", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Class access denied", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/fields")]
pub(crate) async fn get_class_fields(
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<ClassId>,
    options: web::Query<FieldDiscoveryOptions>,
) -> Result<impl Responder, ApiError> {
    discover_class_fields(
        context,
        requestor,
        ClassSelector::by_id(class_id.into_inner()),
        options.into_inner(),
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/fields",
    tag = "classes",
    summary = "Discover fields in an authorized sample of a class",
    description = "Returns observed data paths and enabled shared/caller-personal computed selectors. Objects are sampled by ascending ID after authorization. Counts describe this sample only; repeated array items contribute separate observations. Explicit truncation flags identify sample, candidate, depth, traversal, cardinality, path, and response limits. No exact class-wide count or schema inference is performed.",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Exact globally unique class name, including numeric-looking names"),
        ("limit" = Option<usize>, Query, description = "Maximum authorized sample size; defaults to 100", minimum = 1, maximum = 250),
        ("depth" = Option<usize>, Query, description = "Maximum path depth; defaults to 6", minimum = 0, maximum = 32),
        ("containers" = Option<bool>, Query, description = "Include object and array paths; defaults to false")
    ),
    responses(
        (status = 200, description = "Bounded field observations", body = ClassFieldInventory),
        (status = 400, description = "Invalid discovery options", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Class access denied", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/fields")]
pub(crate) async fn get_class_fields_by_name(
    context: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    options: web::Query<FieldDiscoveryOptions>,
) -> Result<impl Responder, ApiError> {
    discover_class_fields(
        context,
        requestor,
        ClassSelector::by_name(class_name.into_inner()),
        options.into_inner(),
    )
    .await
}

async fn discover_class_fields(
    context: AppContext,
    requestor: Authenticated,
    selector: ClassSelector,
    options: FieldDiscoveryOptions,
) -> Result<impl Responder, ApiError> {
    let target = context.class_service().resolve(selector).await?;
    can!(
        &context,
        &requestor.principal,
        requestor.scopes(),
        [Permissions::ReadClass],
        target.class()
    );
    let inventory = class_fields::discover(&context, &requestor, target.class(), options).await?;
    Ok(ApiResponse::new_private_no_store(inventory, StatusCode::OK))
}
