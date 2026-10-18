use actix_web::{HttpRequest, Responder, get, http::StatusCode, web};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::history::{
    HistoryResponse, parse_as_of, resolve_history_principal_names,
};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::history::HistoryNameCursor;
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::models::{HubuumClassHistory, HubuumClassID, HubuumObjectHistory};
use crate::pagination::{effective_page_limit, finalize_page, pagination_headers};
use crate::permissions::AppContext;
use crate::services::history::{
    class_objects_as_of,
    navigation::{HistoryEntry, HistoryReader},
};
use crate::storage::{StorageClassObjectsAsOfQuery, StorageHistoryResource};
use crate::traits::PrincipalIdAccessor;

/// Retained snapshots do not prove that all historical membership is available.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalPopulationCoverage {
    RetainedHistory,
}

#[derive(Serialize, ToSchema)]
pub struct HistoricalClassObjects {
    pub at: DateTime<Utc>,
    pub class: HistoryResponse<HubuumClassHistory>,
    pub objects: Vec<HistoryResponse<HubuumObjectHistory>>,
    pub coverage: HistoricalPopulationCoverage,
}

#[utoipa::path(
    get, path = "/api/v1/classes/{class_id}/objects/history/as-of", tag = "classes",
    security(("bearer_auth" = [])),
    params(
        ("class_id" = i32, Path, description = "Historical class identity"),
        ("at" = String, Query, description = "Required RFC3339 instant, with full timestamp precision"),
        ("limit" = Option<usize>, Query, description = "Page size, subject to the configured default and maximum"),
        ("cursor" = Option<String>, Query, description = "Continuation bound to instant, class, filters, ordering and authorization token"),
        ("include_total" = Option<bool>, Query, description = "Include exact authorized retained-object count (default true)"),
        ("sort" = Option<String>, Query, description = "history_id (default descending) or revision; prefix - for descending"),
        ("revision" = Option<i64>, Query, description = "Filter the effective historical object revision")
    ),
    responses(
        (status = 200, description = "Effective retained object versions and class/schema snapshot at the same instant. Collection IDs are historical references; expanded collections and computed values are not included. An empty retained page does not prove historical emptiness.", body = HistoricalClassObjects),
        (status = 400, description = "Invalid query or mismatched continuation", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Class history unavailable at this instant", body = ApiErrorResponse)
    )
)]
#[get("/{class_id}/objects/history/as-of")]
pub async fn get_class_objects_as_of(
    context: AppContext,
    requestor: Authenticated,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let class_id = class_id.into_inner();
    let at = parse_as_of(req.query_string())?;
    let (mut options, _) = parse_query_parameter_with_passthrough(req.query_string(), &["at"])?;
    let binding = HistoryNameCursor::for_population(
        class_id,
        at,
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
    options.set_limit(Some(effective_page_limit(&options)?))?;
    let query = StorageClassObjectsAsOfQuery::try_new(class_id, at, options.clone())
        .map_err(|error| error.into_request_error())?;
    let reader = HistoryReader::authorize(
        &context,
        &requestor.principal,
        requestor.scopes(),
        StorageHistoryResource::Class(class_id),
    )
    .await?;
    let HistoryEntry::Class(class) = reader.as_of(at).await? else {
        return Err(ApiError::InternalServerError(
            "History returned an unexpected resource kind".into(),
        ));
    };
    let (rows, total) =
        class_objects_as_of(&context, &requestor.principal, requestor.scopes(), query).await?;
    let page = finalize_page(rows, &options)?;
    let next_cursor = page
        .next_cursor
        .map(|cursor| binding.encode(cursor))
        .transpose()?;
    let mut headers = pagination_headers(&next_cursor, total, effective_page_limit(&options)?);
    headers.insert("cache-control".into(), "private, no-store".into());
    let class_names =
        resolve_history_principal_names(&context, std::slice::from_ref(&class)).await?;
    let object_names = resolve_history_principal_names(&context, &page.items).await?;
    Ok(ApiResponse::new_with_headers(
        HistoricalClassObjects {
            at,
            class: HistoryResponse::new(class, &class_names),
            objects: page
                .items
                .into_iter()
                .map(|row| HistoryResponse::new(row, &object_names))
                .collect(),
            coverage: HistoricalPopulationCoverage::RetainedHistory,
        },
        StatusCode::OK,
        headers,
    ))
}
