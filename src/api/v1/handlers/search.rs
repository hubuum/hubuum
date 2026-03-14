use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, web};
use bytes::Bytes;
use futures_util::stream;
use serde::Serialize;

use crate::api::openapi::ApiErrorResponse;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::{
    UnifiedSearchDoneEvent, UnifiedSearchErrorEvent, UnifiedSearchKind, UnifiedSearchResponse,
    UnifiedSearchStartedEvent, execute_unified_search, execute_unified_search_batch,
    parse_unified_search_query,
};
use crate::utilities::response::json_response;

fn sse_event<T: Serialize>(event: &str, payload: &T) -> Result<Bytes, ApiError> {
    let data = serde_json::to_string(payload).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to serialize SSE payload: {error}"))
    })?;
    Ok(Bytes::from(format!("event: {event}\ndata: {data}\n\n")))
}

#[utoipa::path(
    get,
    path = "/api/v1/search",
    tag = "search",
    security(("bearer_auth" = [])),
    params(
        ("q" = String, Query, description = "Plain-text query string"),
        ("kinds" = Option<String>, Query, description = "Comma-separated kinds: namespace,class,object"),
        ("limit_per_kind" = Option<usize>, Query, description = "Maximum results per kind"),
        ("cursor_namespaces" = Option<String>, Query, description = "Opaque cursor for namespace results"),
        ("cursor_classes" = Option<String>, Query, description = "Opaque cursor for class results"),
        ("cursor_objects" = Option<String>, Query, description = "Opaque cursor for object results"),
        ("search_class_schema" = Option<bool>, Query, description = "Include class schema text in class matching"),
        ("search_object_data" = Option<bool>, Query, description = "Include object JSON string values in object matching")
    ),
    responses(
        (status = 200, description = "Grouped unified search results", body = UnifiedSearchResponse),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("")]
pub async fn get_search(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_unified_search_query(req.query_string())?;
    let response = execute_unified_search(&requestor.user, &pool, &params).await?;
    Ok(json_response(response, StatusCode::OK))
}

#[utoipa::path(
    get,
    path = "/api/v1/search/stream",
    tag = "search",
    security(("bearer_auth" = [])),
    params(
        ("q" = String, Query, description = "Plain-text query string"),
        ("kinds" = Option<String>, Query, description = "Comma-separated kinds: namespace,class,object"),
        ("limit_per_kind" = Option<usize>, Query, description = "Maximum results per kind"),
        ("cursor_namespaces" = Option<String>, Query, description = "Opaque cursor for namespace results"),
        ("cursor_classes" = Option<String>, Query, description = "Opaque cursor for class results"),
        ("cursor_objects" = Option<String>, Query, description = "Opaque cursor for object results"),
        ("search_class_schema" = Option<bool>, Query, description = "Include class schema text in class matching"),
        ("search_object_data" = Option<bool>, Query, description = "Include object JSON string values in object matching")
    ),
    responses(
        (status = 200, description = "Server-sent event stream for unified search"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/stream")]
pub async fn stream_search(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let params = parse_unified_search_query(req.query_string())?;
    let mut events = vec![sse_event(
        "started",
        &UnifiedSearchStartedEvent {
            query: params.query.clone(),
        },
    )?];

    for kind in [
        UnifiedSearchKind::Namespace,
        UnifiedSearchKind::Class,
        UnifiedSearchKind::Object,
    ] {
        if !params.includes(kind) {
            continue;
        }

        match execute_unified_search_batch(&requestor.user, &pool, &params, kind).await {
            Ok(batch) => events.push(sse_event("batch", &batch)?),
            Err(error) => {
                events.push(sse_event(
                    "error",
                    &UnifiedSearchErrorEvent {
                        message: error.to_string(),
                    },
                )?);

                let stream = stream::iter(
                    events
                        .into_iter()
                        .map(Ok::<Bytes, actix_web::Error>)
                        .collect::<Vec<_>>(),
                );
                return Ok(HttpResponse::Ok()
                    .insert_header(("Content-Type", "text/event-stream"))
                    .insert_header(("Cache-Control", "no-cache"))
                    .streaming(stream));
            }
        }
    }

    events.push(sse_event(
        "done",
        &UnifiedSearchDoneEvent {
            query: params.query.clone(),
        },
    )?);

    let stream = stream::iter(
        events
            .into_iter()
            .map(Ok::<Bytes, actix_web::Error>)
            .collect::<Vec<_>>(),
    );

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream"))
        .insert_header(("Cache-Control", "no-cache"))
        .streaming(stream))
}
