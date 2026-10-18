use actix_web::{HttpRequest, Responder, get, http::StatusCode, web};
use hubuum_domain::ClassId;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::history::{HistoryResponse, resolve_history_principal_names};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::history::HistoryNameCursor;
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::models::{HubuumClassID, HubuumObjectHistory};
use crate::pagination::{effective_page_limit, finalize_page, pagination_headers};
use crate::permissions::AppContext;
use crate::services::history::object_history_by_name;
use crate::storage::StorageObjectHistoryNameQuery;
use crate::traits::PrincipalIdAccessor;

#[utoipa::path(
    get,
    path = "/api/v1/classes/{class_id}/objects/history",
    tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Historical class identity"),
        ("name" = String, Query, description = "Exact historical object name; numeric names remain names"),
        ("limit" = Option<usize>, Query, description = "Page size (default 50, maximum 500)"),
        ("cursor" = Option<String>, Query, description = "Opaque name-history continuation cursor"),
        ("include_total" = Option<bool>, Query, description = "Include exact visible total (default true)"),
        ("sort" = Option<String>, Query, description = "history_id (default descending) or revision; prefix - for descending"),
        ("revision" = Option<i64>, Query, description = "Filter by snapshot revision")
    ),
    responses(
        (status = 200, description = "Authorized full snapshots across object identities, including deletion tombstones for unscoped admins. Empty matches return an empty list.", body = [HistoryResponse<HubuumObjectHistory>]),
        (status = 400, description = "Invalid query or mismatched cursor", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Token does not permit object reads", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/objects/history")]
pub async fn get_object_history_by_name(
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let class_id = ClassId::new(class_id.id())?;
    let (mut options, passthrough) =
        parse_query_parameter_with_passthrough(req.query_string(), &["name"])?;
    let name = match passthrough.get("name").map(Vec::as_slice) {
        Some([name]) => name.clone(),
        _ => {
            return Err(ApiError::BadRequest(
                "name must be supplied exactly once".to_string(),
            ));
        }
    };
    let binding = HistoryNameCursor::new(
        class_id,
        &name,
        &options,
        requestor.principal.principal_id(),
        requestor.token_meta.id().id(),
        requestor.scopes(),
    )?;
    let cursor = options
        .cursor()
        .map(|cursor| binding.decode(cursor))
        .transpose()?;
    options.set_cursor(cursor)?;
    let query = StorageObjectHistoryNameQuery::try_new(class_id, name, options.clone())
        .map_err(|error| error.into_request_error())?;
    let (rows, total) =
        object_history_by_name(&context, &requestor.principal, requestor.scopes(), query).await?;
    let page = finalize_page(rows, &options)?;
    let next_cursor = page
        .next_cursor
        .map(|cursor| binding.encode(cursor))
        .transpose()?;
    let headers = pagination_headers(&next_cursor, total, effective_page_limit(&options)?);
    let principal_names = resolve_history_principal_names(&context, &page.items).await?;
    let rows = page
        .items
        .into_iter()
        .map(|row| HistoryResponse::new(row, &principal_names))
        .collect::<Vec<_>>();
    Ok(ApiResponse::new_with_headers(rows, StatusCode::OK, headers))
}
