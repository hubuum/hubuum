use super::*;
use crate::api::v1::handlers::history::parse_as_of;
use crate::models::HistoricalComputedValues;
use crate::services::history::computed;

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/{object_id}/history/computed/as-of", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Current class identity"),
        ("object_id" = i32, Path, description = "Object identity"),
        ("at" = String, Query, description = "Required RFC 3339 historical instant")),
    responses((status = 200, description = "Historical recomputation using retained shared definitions; never recorded materialization output. Personal fields are unsupported. Missing definition history and unsupported semantics are explicit; field errors have no value.", body = HistoricalComputedValues),
        (status = 400, description = "Invalid query", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Current or historical object/class access denied", body = ApiErrorResponse),
        (status = 404, description = "Object history unavailable", body = ApiErrorResponse)))]
#[get("/{class_id}/{object_id}/history/computed/as-of")]
pub async fn get_object_computed_as_of(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id) = path.into_inner();
    let at = parse_as_of(req.query_string())?;
    let response = computed::as_of(
        &context,
        &requestor.principal,
        requestor.scopes(),
        class_id,
        object_id,
        at,
    )
    .await?;
    Ok(ApiResponse::new_private_no_store(response, StatusCode::OK))
}
