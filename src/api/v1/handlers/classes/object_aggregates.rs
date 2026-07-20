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
use crate::models::{
    HubuumClassID, ObjectAggregateAuthorization, ObjectAggregateBackendRequest,
    ObjectAggregateCursorBudget, ObjectAggregateRow, ObjectAggregateTarget, Permissions, UserID,
};
use crate::pagination::effective_page_limit;
use crate::permissions::AppContext;
use crate::traits::{Search, SelfAccessors};

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/object-aggregates",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Class ID"),
        ("group_by" = Vec<String>, Query, description = "One to three repeated ordered dimensions: name, description, collection_id, created_at, updated_at, json_data.<comma-separated-path>, computed.shared.<key>, or computed.personal.<key>"),
        ("sort" = Option<String>, Query, description = "Aggregate ordering: dimensions.asc, dimensions.desc, object_count.asc, or object_count.desc")
    ),
    responses(
        (status = 200, description = "Permission-scoped aggregated object counts. Value states distinguish value, JSON null, a missing JSON path, and an unavailable computed result.", body = [ObjectAggregateRow]),
        (status = 400, description = "Invalid dimension, path, sort, cursor, or computed selector", body = ApiErrorResponse),
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
    let user = &requestor.principal;
    let class_id = class_id.into_inner();
    let class = class_id.instance(&pool).await?;
    let target = ObjectAggregateTarget::from_class(&class)?;
    let query = parse_object_aggregate_query(req.query_string())?;
    let cursor_budget =
        ObjectAggregateCursorBudget::for_request_target(req.path(), req.query_string())?;

    let personal_owner_id = if query.spec().has_personal_computed_dimension() {
        if !requestor.principal.is_human() {
            return Err(ApiError::BadRequest(
                "Service accounts cannot group by personal computed fields".to_string(),
            ));
        }
        Some(UserID::new(
            computed_personal_owner(&pool, &requestor, &class)
                .await?
                .ok_or_else(|| {
                    ApiError::BadRequest(
                        "Personal computed grouping requires ReadClass access to the requested class"
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
    required.ensure_contains(&[Permissions::ReadObject]);
    let required = required.iter().copied().collect::<Vec<_>>();
    let authorization = ObjectAggregateAuthorization::new(
        required,
        requestor.scopes().map(<[Permissions]>::to_vec),
    )?;

    let computed = query.spec().has_computed_dimension();
    let effective_limit = effective_page_limit(query.query_options())?;
    let mut request = ObjectAggregateBackendRequest::builder(target, query)
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
