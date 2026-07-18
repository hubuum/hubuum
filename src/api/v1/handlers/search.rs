use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode};
use bytes::Bytes;
use futures_util::stream;
use serde::Serialize;
use std::collections::HashMap;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::{
    UnifiedSearchDoneEvent, UnifiedSearchErrorEvent, UnifiedSearchKind, UnifiedSearchResponse,
    UnifiedSearchStartedEvent, execute_unified_search, execute_unified_search_batch,
    parse_unified_search_query,
};
use crate::pagination::PAGE_LIMIT_HEADER;
use crate::permissions::AppContext;

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
        ("kinds" = Option<String>, Query, description = "Comma-separated kinds: collection,class,object"),
        ("limit_per_kind" = Option<usize>, Query, description = "Maximum results per kind"),
        ("cursor_collections" = Option<String>, Query, description = "Opaque cursor for collection results"),
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
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_unified_search_query(req.query_string())?;
    let response =
        execute_unified_search(&requestor.principal, &pool, &params, requestor.scopes()).await?;
    Ok(ApiResponse::new_with_headers(
        response,
        StatusCode::OK,
        HashMap::from([(
            PAGE_LIMIT_HEADER.to_string(),
            params.limit_per_kind.to_string(),
        )]),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/search/stream",
    tag = "search",
    security(("bearer_auth" = [])),
    params(
        ("q" = String, Query, description = "Plain-text query string"),
        ("kinds" = Option<String>, Query, description = "Comma-separated kinds: collection,class,object"),
        ("limit_per_kind" = Option<usize>, Query, description = "Maximum results per kind"),
        ("cursor_collections" = Option<String>, Query, description = "Opaque cursor for collection results"),
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
    pool: AppContext,
    requestor: Authenticated,
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
        UnifiedSearchKind::Collection,
        UnifiedSearchKind::Class,
        UnifiedSearchKind::Object,
    ] {
        if !params.includes(kind) {
            continue;
        }

        match execute_unified_search_batch(
            &requestor.principal,
            &pool,
            &params,
            kind,
            requestor.scopes(),
        )
        .await
        {
            Ok(batch) => events.push(sse_event("batch", &batch)?),
            Err(error) => {
                events.push(sse_event(
                    "error",
                    &UnifiedSearchErrorEvent {
                        message: error.public_message().to_string(),
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
                    .insert_header((PAGE_LIMIT_HEADER, params.limit_per_kind.to_string()))
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
        .insert_header((PAGE_LIMIT_HEADER, params.limit_per_kind.to_string()))
        .streaming(stream))
}
