use actix_web::{HttpRequest, Responder, routes, web};
// `routes` consumes these method attributes before rustc records the import.
#[allow(unused_imports)]
use actix_web::get;
use tracing::debug;

use super::computed_personal_owner;
use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::object_aggregate::parse_object_aggregate_query;
use crate::models::search::QueryParamsExt;
use crate::models::traits::ResolveClassTarget;
use crate::models::{
    ClassSelector, HubuumClassID, ObjectAggregateAuthorization, ObjectAggregateBackendRequest,
    ObjectAggregateCursorBudget, ObjectAggregateRow, ObjectAggregateTarget, Permissions,
    ResolvedClassTarget, UserID,
};
use crate::pagination::effective_page_limit;
use crate::permissions::AppContext;
use crate::traits::{Search, SelfAccessors};

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/object-aggregates",
    tag = "classes",
    description = "Permission-scoped object aggregation. Normal object filters and up to two enabled shared or owned personal computed filters are applied before optional grouping and numeric measures.",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("group_by" = Option<Vec<String>>, Query, description = "Up to three repeated ordered dimensions: name, description, collection_id, created_at, updated_at, json_data.<comma-separated-path>, computed.shared.<key>, or computed.personal.<key>"),
        ("aggregate" = Option<Vec<String>>, Query, description = "Up to four repeated numeric measures in operation:field form. Operations are sum, average, min, and max; fields are json_data.<comma-separated-path>, computed.shared.<key>, or computed.personal.<key>. At least one group_by or aggregate parameter is required."),
        ("sort" = Option<String>, Query, description = "Aggregate ordering: dimensions.asc, dimensions.desc, object_count.asc, or object_count.desc")
    ),
    responses(
        (status = 200, description = "Permission-scoped grouped or global aggregate rows. Dimension states preserve null, missing, and unavailable values; numeric measures report contributing and skipped source counts.", body = [ObjectAggregateRow]),
        (status = 400, description = "Invalid filter, dimension, measure, path, sort, cursor, or computed selector", body = ApiErrorResponse),
        (status = 413, description = "An aggregate value is too large for a replay-safe cursor, or source snapshots or externally authorized intermediate aggregates exceed their memory bounds", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[routes]
#[get("/{class_id}/object-aggregates")]
#[get("/{class_id}/object-aggregates/")]
pub(crate) async fn get_object_aggregates(
    pool: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_id(class_id.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_object_aggregates(pool, requestor, target, req).await
}

#[utoipa::path(
    get,
    path = "/api/v1/classes/by-name/{class_name}/object-aggregates",
    tag = "classes",
    summary = "Aggregate objects in a class by class name",
    description = "Name-addressed alias for permission-scoped object aggregation. Numeric-looking class names remain names. Normal object filters and up to two enabled shared or owned personal computed filters are applied before optional grouping and numeric measures.",
    security(("bearer_auth" = [])),
    params(
        ("class_name" = String, Path, description = "Globally unique class name"),
        ("group_by" = Option<Vec<String>>, Query, description = "Up to three repeated ordered dimensions: name, description, collection_id, created_at, updated_at, json_data.<comma-separated-path>, computed.shared.<key>, or computed.personal.<key>"),
        ("aggregate" = Option<Vec<String>>, Query, description = "Up to four repeated numeric measures in operation:field form. Operations are sum, average, min, and max; fields are json_data.<comma-separated-path>, computed.shared.<key>, or computed.personal.<key>. At least one group_by or aggregate parameter is required."),
        ("sort" = Option<String>, Query, description = "Aggregate ordering: dimensions.asc, dimensions.desc, object_count.asc, or object_count.desc")
    ),
    responses(
        (status = 200, description = "Permission-scoped grouped or global aggregate rows. Dimension states preserve null, missing, and unavailable values; numeric measures report contributing and skipped source counts.", body = [ObjectAggregateRow]),
        (status = 400, description = "Invalid filter, dimension, measure, path, sort, cursor, or computed selector", body = ApiErrorResponse),
        (status = 413, description = "An aggregate value is too large for a replay-safe cursor, or source snapshots or externally authorized intermediate aggregates exceed their memory bounds", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 404, description = "Class not found", body = ApiErrorResponse)
    )
)]
#[get("/by-name/{class_name}/object-aggregates")]
pub(crate) async fn get_object_aggregates_by_name(
    pool: AppContext,
    requestor: Authenticated,
    class_name: web::Path<String>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let target = ClassSelector::by_name(class_name.into_inner())
        .resolve_class_target(&pool)
        .await?;
    read_object_aggregates(pool, requestor, target, req).await
}

async fn read_object_aggregates(
    pool: AppContext,
    requestor: Authenticated,
    class_target: ResolvedClassTarget,
    req: HttpRequest,
) -> Result<ApiResponse<Vec<ObjectAggregateRow>>, ApiError> {
    let user = &requestor.principal;
    let class = class_target.class();
    let aggregate_target = ObjectAggregateTarget::from_class(class)?;
    let query = parse_object_aggregate_query(req.query_string())?;
    let computed = query.uses_computed_values();
    let cursor_budget =
        ObjectAggregateCursorBudget::for_request_target(req.path(), req.query_string())?;

    let personal_owner_id = if query.requires_personal_owner() {
        if !requestor.principal.is_human() {
            return Err(ApiError::BadRequest(
                "Service accounts cannot filter, group by, or measure personal computed fields"
                    .to_string(),
            ));
        }
        Some(UserID::new(
            computed_personal_owner(&pool, &requestor, class)
                .await?
                .ok_or_else(|| {
                    ApiError::BadRequest(
                        "Personal computed aggregation requires ReadClass access to the requested class"
                            .to_string(),
                    )
                })?,
        )?)
    } else {
        None
    };

    debug!(
        message = "Getting object aggregates in class",
        user_id = user.id(),
        class_id = class.id,
        query = req.query_string()
    );

    let mut required = query.query_options().filters.permissions()?;
    required.ensure_contains(&[Permissions::ReadObject, Permissions::ReadCollection]);
    let required = required.iter().copied().collect::<Vec<_>>();
    let authorization = ObjectAggregateAuthorization::new(
        required,
        requestor.scopes().map(<[Permissions]>::to_vec),
    )?;

    let effective_limit = effective_page_limit(query.query_options())?;
    let mut request = ObjectAggregateBackendRequest::builder(aggregate_target, query)
        .authorization(authorization)
        .cursor_budget(cursor_budget);
    if let Some(owner_id) = personal_owner_id {
        request = request.personal_owner(owner_id);
    }
    let page = user.aggregate_objects(&pool, request.build()?).await?;
    let (rows, total_count, next_cursor) = page.into_parts();
    Ok(ApiResponse::paginated_items(
        rows,
        &next_cursor,
        total_count,
        effective_limit,
        computed,
    ))
}
