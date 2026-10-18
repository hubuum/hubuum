use actix_web::{HttpRequest, Responder, get, http::StatusCode, web};
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use super::history_population::HistoricalPopulationCoverage;
use crate::api::v1::handlers::history::{
    HistoryResponse, parse_as_of, resolve_history_principal_names,
};
use crate::api::{openapi::ApiErrorResponse, response::ApiResponse};
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::history::HistoryNameCursor;
use crate::models::search::parse_query_parameter_with_passthrough;
use crate::models::{HistoricalRelation, HubuumClassID, HubuumObjectID};
use crate::pagination::{effective_page_limit, finalize_page, pagination_headers};
use crate::permissions::AppContext;
use crate::services::history::navigation::HistoryReader;
use crate::storage::{StorageHistoryResource, StorageRelationHistorySelection};
use crate::traits::PrincipalIdAccessor;

#[derive(Serialize, ToSchema)]
pub struct HistoricalRelationships {
    /// Null for the change stream; each entry supplies its own context_at.
    pub at: Option<DateTime<Utc>>,
    pub relations: Vec<HistoryResponse<HistoricalRelation>>,
    pub coverage: HistoricalPopulationCoverage,
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/relations/history", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class identity"),
        ("limit" = Option<usize>, Query, description = "Configured page size"),
        ("cursor" = Option<String>, Query, description = "Continuation bound to root, selection, ordering and token"),
        ("include_total" = Option<bool>, Query, description = "Exact authorized count; default true"),
        ("sort" = Option<String>, Query, description = "history_id or -history_id (default); filters are unsupported")),
    responses((status = 200, description = "Retained direct relationship changes, including deletion markers for unscoped administrators. Endpoints are resolved at each change's valid_from.", body = HistoricalRelationships),
        (status = 400, description = "Invalid query or cursor", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History root unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/relations/history")]
pub async fn get_class_relation_history(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    respond(
        &context,
        &requestor,
        StorageHistoryResource::Class(path.into_inner()),
        StorageRelationHistorySelection::Changes,
        &req,
    )
    .await
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/relations/history/as-of", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Class identity"),
        ("at" = String, Query, description = "Required RFC3339 instant; half-open validity with full timestamp precision"),
        ("limit" = Option<usize>, Query, description = "Configured page size"),
        ("cursor" = Option<String>, Query, description = "Continuation bound to root, instant, ordering and token"),
        ("include_total" = Option<bool>, Query, description = "Exact authorized count; default true"),
        ("sort" = Option<String>, Query, description = "history_id or -history_id (default); filters are unsupported")),
    responses((status = 200, description = "Active retained direct class edges and endpoint context at the requested instant. No transitive traversal or live-context fallback.", body = HistoricalRelationships),
        (status = 400, description = "Invalid query or cursor", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History root unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/relations/history/as-of")]
pub async fn get_class_relations_as_of(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    respond(
        &context,
        &requestor,
        StorageHistoryResource::Class(path.into_inner()),
        StorageRelationHistorySelection::At(parse_as_of(req.query_string())?),
        &req,
    )
    .await
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/{object_id}/relations/history", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Current class identity, or historical class for a deleted object"),
        ("object_id" = i32, Path, description = "Object identity"),
        ("limit" = Option<usize>, Query, description = "Configured page size"),
        ("cursor" = Option<String>, Query, description = "Continuation bound to root, selection, ordering and token"),
        ("include_total" = Option<bool>, Query, description = "Exact authorized count; default true"),
        ("sort" = Option<String>, Query, description = "history_id or -history_id (default); filters are unsupported")),
    responses((status = 200, description = "Retained direct object relationship changes. Endpoints are resolved at each change's valid_from; class_relation_id is a historical reference without expanded labels.", body = HistoricalRelationships),
        (status = 400, description = "Invalid query or cursor", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History root unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/{object_id}/relations/history")]
pub async fn get_object_relation_history(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id) = path.into_inner();
    respond(
        &context,
        &requestor,
        StorageHistoryResource::Object {
            class_id,
            object_id,
        },
        StorageRelationHistorySelection::Changes,
        &req,
    )
    .await
}

#[utoipa::path(get, path = "/api/v1/classes/{class_id}/{object_id}/relations/history/as-of", tag = "classes", security(("bearer_auth" = [])),
    params(("class_id" = i32, Path, description = "Current class identity, or historical class for a deleted object"),
        ("object_id" = i32, Path, description = "Object identity"),
        ("at" = String, Query, description = "Required RFC3339 instant; half-open validity with full timestamp precision"),
        ("limit" = Option<usize>, Query, description = "Configured page size"),
        ("cursor" = Option<String>, Query, description = "Continuation bound to root, instant, ordering and token"),
        ("include_total" = Option<bool>, Query, description = "Exact authorized count; default true"),
        ("sort" = Option<String>, Query, description = "history_id or -history_id (default); filters are unsupported")),
    responses((status = 200, description = "Active retained direct object edges and historical endpoint names, classes and collection IDs at the requested instant. No transitive traversal or live-context fallback.", body = HistoricalRelationships),
        (status = 400, description = "Invalid query or cursor", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "History root unavailable", body = ApiErrorResponse))) ]
#[get("/{class_id}/{object_id}/relations/history/as-of")]
pub async fn get_object_relations_as_of(
    context: AppContext,
    requestor: Authenticated,
    path: web::Path<(HubuumClassID, HubuumObjectID)>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let (class_id, object_id) = path.into_inner();
    respond(
        &context,
        &requestor,
        StorageHistoryResource::Object {
            class_id,
            object_id,
        },
        StorageRelationHistorySelection::At(parse_as_of(req.query_string())?),
        &req,
    )
    .await
}

async fn respond(
    context: &AppContext,
    requestor: &Authenticated,
    resource: StorageHistoryResource,
    selection: StorageRelationHistorySelection,
    req: &HttpRequest,
) -> Result<ApiResponse<HistoricalRelationships>, ApiError> {
    let passthrough = match selection {
        StorageRelationHistorySelection::Changes => &[][..],
        StorageRelationHistorySelection::At(_) => &["at"][..],
    };
    let (mut options, _) = parse_query_parameter_with_passthrough(req.query_string(), passthrough)?;
    let binding = HistoryNameCursor::for_relationships(
        resource,
        selection,
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
    let reader =
        HistoryReader::authorize(context, &requestor.principal, requestor.scopes(), resource)
            .await?;
    let (rows, total) = reader.relationships(selection, options.clone()).await?;
    let page = finalize_page(rows, &options)?;
    let next_cursor = page
        .next_cursor
        .map(|cursor| binding.encode(cursor))
        .transpose()?;
    let mut headers = pagination_headers(&next_cursor, total, effective_page_limit(&options)?);
    headers.insert("cache-control".into(), "private, no-store".into());
    let names = resolve_history_principal_names(context, &page.items).await?;
    Ok(ApiResponse::new_with_headers(
        HistoricalRelationships {
            at: match selection {
                StorageRelationHistorySelection::Changes => None,
                StorageRelationHistorySelection::At(at) => Some(at),
            },
            relations: page
                .items
                .into_iter()
                .map(|row| HistoryResponse::new(row, &names))
                .collect(),
            coverage: HistoricalPopulationCoverage::RetainedHistory,
        },
        StatusCode::OK,
        headers,
    ))
}
