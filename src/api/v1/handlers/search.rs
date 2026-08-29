use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode};
use bytes::Bytes;
use futures_util::{
    FutureExt, Stream, StreamExt,
    future::BoxFuture,
    stream::{self, FuturesUnordered},
};
use serde::Serialize;
use std::collections::HashMap;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::errors::ApiError;
use crate::extractors::Authenticated;
use crate::models::{
    StorageUnifiedSearchQuery, TokenScope, UnifiedSearchBatchResponse, UnifiedSearchDoneEvent,
    UnifiedSearchErrorEvent, UnifiedSearchKind, UnifiedSearchResponse, UnifiedSearchStartedEvent,
    execute_unified_search, execute_unified_search_batch, parse_unified_search_query,
};
use crate::pagination::PAGE_LIMIT_HEADER;
use crate::permissions::AppContext;
use crate::storage::StorageAuthenticationPrincipal;

fn sse_event<T: Serialize>(event: &str, payload: &T) -> Result<Bytes, ApiError> {
    let data = serde_json::to_string(payload).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to serialize SSE payload: {error}"))
    })?;
    Ok(Bytes::from(format!("event: {event}\ndata: {data}\n\n")))
}

type UnifiedSearchBatchFuture = BoxFuture<'static, Result<UnifiedSearchBatchResponse, ApiError>>;

enum UnifiedSearchEventStreamPhase {
    Starting,
    Searching,
    Finished,
}

struct UnifiedSearchEventStreamState {
    query: String,
    searches: FuturesUnordered<UnifiedSearchBatchFuture>,
    phase: UnifiedSearchEventStreamPhase,
}

fn search_event_stream(
    query: String,
    searches: FuturesUnordered<UnifiedSearchBatchFuture>,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> {
    let state = UnifiedSearchEventStreamState {
        query,
        searches,
        phase: UnifiedSearchEventStreamPhase::Starting,
    };

    stream::unfold(state, |mut state| async move {
        match state.phase {
            UnifiedSearchEventStreamPhase::Starting => {
                state.phase = UnifiedSearchEventStreamPhase::Searching;
                let event = sse_event(
                    "started",
                    &UnifiedSearchStartedEvent {
                        query: state.query.clone(),
                    },
                )
                .map_err(actix_web::Error::from);
                Some((event, state))
            }
            UnifiedSearchEventStreamPhase::Searching => match state.searches.next().await {
                Some(Ok(batch)) => {
                    let event = sse_event("batch", &batch).map_err(actix_web::Error::from);
                    Some((event, state))
                }
                Some(Err(error)) => {
                    state.searches.clear();
                    state.phase = UnifiedSearchEventStreamPhase::Finished;
                    let event = sse_event(
                        "error",
                        &UnifiedSearchErrorEvent {
                            message: error.public_message().to_string(),
                        },
                    )
                    .map_err(actix_web::Error::from);
                    Some((event, state))
                }
                None => {
                    state.phase = UnifiedSearchEventStreamPhase::Finished;
                    let event = sse_event(
                        "done",
                        &UnifiedSearchDoneEvent {
                            query: state.query.clone(),
                        },
                    )
                    .map_err(actix_web::Error::from);
                    Some((event, state))
                }
            },
            UnifiedSearchEventStreamPhase::Finished => None,
        }
    })
}

fn execute_unified_search_stream(
    context: AppContext,
    principal: StorageAuthenticationPrincipal,
    scope: Option<TokenScope>,
    params: StorageUnifiedSearchQuery,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> {
    let searches = FuturesUnordered::new();

    for kind in [
        UnifiedSearchKind::Collection,
        UnifiedSearchKind::Class,
        UnifiedSearchKind::Object,
    ] {
        if !params.includes(kind) {
            continue;
        }

        let context = context.clone();
        let principal = principal.clone();
        let scope = scope.clone();
        let params = params.clone();
        searches.push(
            async move {
                execute_unified_search_batch(&principal, &context, &params, kind, scope.as_ref())
                    .await
            }
            .boxed(),
        );
    }

    search_event_stream(params.query, searches)
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
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let params = parse_unified_search_query(req.query_string())?;
    let response =
        execute_unified_search(&requestor.principal, &context, &params, requestor.scopes()).await?;
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
        (status = 200, description = "Server-sent event stream for unified search", content_type = "text/event-stream"),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse)
    )
)]
#[get("/stream")]
pub async fn stream_search(
    context: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let params = parse_unified_search_query(req.query_string())?;
    let limit_per_kind = params.limit_per_kind;
    let stream =
        execute_unified_search_stream(context, requestor.principal, requestor.scope, params);

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream; charset=utf-8"))
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("X-Accel-Buffering", "no"))
        .insert_header((PAGE_LIMIT_HEADER, limit_per_kind.to_string()))
        .streaming(stream))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::channel::oneshot;
    use futures_util::{FutureExt, StreamExt, future, pin_mut};

    use super::*;

    fn empty_batch(kind: &str) -> UnifiedSearchBatchResponse {
        UnifiedSearchBatchResponse {
            kind: kind.to_string(),
            collections: vec![],
            classes: vec![],
            objects: vec![],
            next: None,
        }
    }

    struct DropNotifier(Option<oneshot::Sender<()>>);

    impl Drop for DropNotifier {
        fn drop(&mut self) {
            if let Some(notify) = self.0.take() {
                let _ = notify.send(());
            }
        }
    }

    #[actix_web::test]
    async fn search_stream_emits_started_before_search_completes() {
        let (release, blocked) = oneshot::channel::<()>();
        let searches = FuturesUnordered::new();
        searches.push(
            async move {
                blocked.await.unwrap();
                Ok(empty_batch("objects"))
            }
            .boxed(),
        );
        let events = search_event_stream("needle".to_string(), searches);
        pin_mut!(events);

        let started = events.next().await.unwrap().unwrap();
        assert!(
            String::from_utf8(started.to_vec())
                .unwrap()
                .contains("event: started")
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(10), events.next())
                .await
                .is_err()
        );

        release.send(()).unwrap();
    }

    #[actix_web::test]
    async fn search_stream_emits_batches_in_completion_order() {
        let (release_classes, blocked_classes) = oneshot::channel::<()>();
        let (release_objects, blocked_objects) = oneshot::channel::<()>();
        let searches = FuturesUnordered::new();
        searches.push(
            async move {
                blocked_classes.await.unwrap();
                Ok(empty_batch("classes"))
            }
            .boxed(),
        );
        searches.push(
            async move {
                blocked_objects.await.unwrap();
                Ok(empty_batch("objects"))
            }
            .boxed(),
        );
        let events = search_event_stream("needle".to_string(), searches);
        pin_mut!(events);

        events.next().await.unwrap().unwrap();
        release_objects.send(()).unwrap();
        let first_batch = events.next().await.unwrap().unwrap();

        assert!(
            String::from_utf8(first_batch.to_vec())
                .unwrap()
                .contains("\"kind\":\"objects\"")
        );

        release_classes.send(()).unwrap();
    }

    #[actix_web::test]
    async fn search_stream_error_is_terminal() {
        let searches = FuturesUnordered::new();
        searches.push(
            async {
                Err(ApiError::BadRequest(
                    "search batch deliberately failed".to_string(),
                ))
            }
            .boxed(),
        );
        let events = search_event_stream("needle".to_string(), searches);
        pin_mut!(events);

        events.next().await.unwrap().unwrap();
        let error = events.next().await.unwrap().unwrap();
        let error = String::from_utf8(error.to_vec()).unwrap();
        assert!(error.contains("event: error"));
        assert!(error.contains("search batch deliberately failed"));
        assert!(events.next().await.is_none());
    }

    #[actix_web::test]
    async fn dropping_search_stream_drops_pending_batches() {
        let (notify_drop, drop_observed) = oneshot::channel();
        let searches = FuturesUnordered::new();
        searches.push(
            async move {
                let _drop_notifier = DropNotifier(Some(notify_drop));
                future::pending::<Result<UnifiedSearchBatchResponse, ApiError>>().await
            }
            .boxed(),
        );
        let mut events = Box::pin(search_event_stream("needle".to_string(), searches));

        events.next().await.unwrap().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), events.next())
                .await
                .is_err()
        );
        drop(events);

        tokio::time::timeout(Duration::from_secs(1), drop_observed)
            .await
            .unwrap()
            .unwrap();
    }
}
