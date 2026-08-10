use actix_web::{HttpRequest, HttpResponse, Responder, get, http::StatusCode, post};
use bytes::Bytes;
use futures_util::{
    FutureExt, Stream, StreamExt,
    future::{BoxFuture, LocalBoxFuture},
    stream::{self, FuturesUnordered},
};
use serde::Serialize;
use std::collections::HashMap;

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::api::v1::handlers::events::visible_event_scope;
use crate::can;
use crate::db::traits::authz::{AuthzSubject, scope_allows};
use crate::db::traits::events::list_structured_events_with_total_count;
use crate::db::traits::principal::load_principal_with_user;
use crate::db::traits::service_account::{
    count_structured_manageable_service_accounts, search_structured_manageable_service_accounts,
};
use crate::db::traits::user::search::{
    ExternalRelatedFilterAuthorization, externally_authorized_structured_objects,
};
use crate::db::traits::user::{UserPermissions, UserSearchBackend};
use crate::errors::ApiError;
use crate::extractors::{Authenticated, StructuredSearchPayload};
use crate::models::traits::ResolveClassTarget;
use crate::models::{
    Group, GroupResponse, MAX_STRUCTURED_SEARCH_EXTERNAL_CANDIDATES, Permissions, Principal,
    ServiceAccountResponse, ServiceAccountWithName, StructuredSearchDoneEvent,
    StructuredSearchErrorEvent, StructuredSearchRequest, StructuredSearchResourceKind,
    StructuredSearchResponse, StructuredSearchResult, StructuredSearchStartedEvent, TokenScope,
    UnifiedSearchBatchResponse, UnifiedSearchDoneEvent, UnifiedSearchErrorEvent, UnifiedSearchKind,
    UnifiedSearchQuery, UnifiedSearchResponse, UnifiedSearchStartedEvent, User, UserResponse,
    UserWithName, decode_structured_search_cursor, encode_structured_search_cursor,
    execute_unified_search, execute_unified_search_batch, parse_unified_search_query,
};
use crate::pagination::{
    NEXT_CURSOR_HEADER, PAGE_LIMIT_HEADER, TOTAL_COUNT_HEADER, count_query_options,
    effective_page_limit, finalize_page, paginate_in_memory, prepare_db_pagination,
};
use crate::permissions::visibility::authorize_cursor_page;
use crate::permissions::{AppContext, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef};
use crate::traits::{BackendContext, CursorPaginated, Search, SelfAccessors};

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
    pool: AppContext,
    principal: Principal,
    scope: Option<TokenScope>,
    params: UnifiedSearchQuery,
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

        let pool = pool.clone();
        let principal = principal.clone();
        let scope = scope.clone();
        let params = params.clone();
        searches.push(
            async move {
                execute_unified_search_batch(&principal, &pool, &params, kind, scope.as_ref()).await
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

pub(crate) struct StructuredSearchExecution {
    pub(crate) response: StructuredSearchResponse,
    pub(crate) page_limit: usize,
}

type StructuredSearchFuture = LocalBoxFuture<'static, Result<StructuredSearchExecution, ApiError>>;

enum StructuredSearchEventStreamPhase {
    Starting(StructuredSearchFuture),
    Searching(StructuredSearchFuture),
    Delivering {
        results: std::vec::IntoIter<StructuredSearchResult>,
        done: StructuredSearchDoneEvent,
    },
    Finished,
}

struct StructuredSearchEventStreamState {
    version: u8,
    kind: StructuredSearchResourceKind,
    phase: StructuredSearchEventStreamPhase,
}

fn structured_search_event_stream(
    version: u8,
    kind: StructuredSearchResourceKind,
    execution: StructuredSearchFuture,
) -> impl Stream<Item = Result<Bytes, actix_web::Error>> {
    let state = StructuredSearchEventStreamState {
        version,
        kind,
        phase: StructuredSearchEventStreamPhase::Starting(execution),
    };

    stream::unfold(state, |mut state| async move {
        loop {
            let phase =
                std::mem::replace(&mut state.phase, StructuredSearchEventStreamPhase::Finished);
            match phase {
                StructuredSearchEventStreamPhase::Starting(execution) => {
                    state.phase = StructuredSearchEventStreamPhase::Searching(execution);
                    let event = sse_event(
                        "started",
                        &StructuredSearchStartedEvent {
                            version: state.version,
                            kind: state.kind,
                        },
                    )
                    .map_err(actix_web::Error::from);
                    return Some((event, state));
                }
                StructuredSearchEventStreamPhase::Searching(execution) => match execution.await {
                    Ok(execution) => {
                        let StructuredSearchExecution {
                            response,
                            page_limit,
                        } = execution;
                        let StructuredSearchResponse {
                            version,
                            kind,
                            results,
                            next,
                            total,
                        } = response;
                        state.phase = StructuredSearchEventStreamPhase::Delivering {
                            results: results.into_iter(),
                            done: StructuredSearchDoneEvent {
                                version,
                                kind,
                                next,
                                total,
                                page_limit,
                            },
                        };
                    }
                    Err(error) => {
                        state.phase = StructuredSearchEventStreamPhase::Finished;
                        let event = sse_event(
                            "error",
                            &StructuredSearchErrorEvent {
                                version: state.version,
                                kind: state.kind,
                                message: error.public_message().to_string(),
                            },
                        )
                        .map_err(actix_web::Error::from);
                        return Some((event, state));
                    }
                },
                StructuredSearchEventStreamPhase::Delivering { mut results, done } => {
                    if let Some(result) = results.next() {
                        state.phase =
                            StructuredSearchEventStreamPhase::Delivering { results, done };
                        let event = sse_event("result", &result).map_err(actix_web::Error::from);
                        return Some((event, state));
                    }
                    state.phase = StructuredSearchEventStreamPhase::Finished;
                    let event = sse_event("done", &done).map_err(actix_web::Error::from);
                    return Some((event, state));
                }
                StructuredSearchEventStreamPhase::Finished => return None,
            }
        }
    })
}

fn finalize_structured_search<T, F>(
    rows: Vec<T>,
    total: Option<i64>,
    context: StructuredSearchPageContext<'_>,
    map_result: F,
) -> Result<StructuredSearchExecution, ApiError>
where
    T: CursorPaginated,
    F: FnMut(T) -> StructuredSearchResult,
{
    let page = finalize_page(rows, context.query_options)?;
    let next = page
        .next_cursor
        .map(|cursor| {
            encode_structured_search_cursor(context.fingerprint, cursor, context.cursor_budget)
        })
        .transpose()?;
    Ok(StructuredSearchExecution {
        response: StructuredSearchResponse {
            version: context.version,
            kind: context.kind,
            results: page.items.into_iter().map(map_result).collect(),
            next,
            total,
        },
        page_limit: effective_page_limit(context.query_options)?,
    })
}

#[derive(Clone, Copy)]
struct StructuredSearchPageContext<'a> {
    query_options: &'a crate::models::search::QueryOptions,
    fingerprint: &'a str,
    cursor_budget: usize,
    version: u8,
    kind: StructuredSearchResourceKind,
}

fn ensure_external_candidate_limit(count: usize) -> Result<(), ApiError> {
    if count > MAX_STRUCTURED_SEARCH_EXTERNAL_CANDIDATES {
        return Err(ApiError::BadRequest(format!(
            "Structured search produced more than {MAX_STRUCTURED_SEARCH_EXTERNAL_CANDIDATES} external-policy candidates; narrow the query"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct StructuredAuthorizationContext<'a> {
    pool: &'a AppContext,
    principal: &'a Principal,
    scopes: Option<&'a TokenScope>,
    include_total: bool,
}

async fn authorize_structured_candidates<T, F>(
    context: StructuredAuthorizationContext<'_>,
    candidates: Vec<T>,
    query_options: &crate::models::search::QueryOptions,
    permissions: Vec<Permissions>,
    to_resource: F,
) -> Result<(Vec<T>, Option<i64>), ApiError>
where
    T: CursorPaginated,
    F: Fn(&T) -> ResourceRef,
{
    ensure_external_candidate_limit(candidates.len())?;
    let policy_principal = PrincipalRef::load(context.pool, context.principal).await?;
    let prepared = prepare_db_pagination::<T>(query_options)?;
    let page = authorize_cursor_page(
        context.pool.permission_backend(),
        &policy_principal,
        candidates,
        context.scopes,
        permissions,
        &prepared,
        to_resource,
    )
    .await?;
    Ok((page.rows, context.include_total.then_some(page.total_count)))
}

async fn structured_iam_user(
    pool: &AppContext,
    principal: &Principal,
    scopes: Option<&TokenScope>,
    target: StructuredSearchResourceKind,
) -> Result<User, ApiError> {
    if scopes.is_some() || !principal.is_human() {
        return Err(ApiError::Forbidden(format!(
            "{} search requires a human principal with an unscoped token",
            target.as_str()
        )));
    }
    let (_, user) = load_principal_with_user(pool.db_pool(), principal.id()).await?;
    user.ok_or_else(|| {
        ApiError::InternalServerError("Human principal does not have a user record".to_string())
    })
}

pub(crate) async fn execute_structured_search(
    pool: &AppContext,
    principal: &Principal,
    token_id: i32,
    token_revision: i64,
    scopes: Option<&TokenScope>,
    request: StructuredSearchRequest,
) -> Result<StructuredSearchExecution, ApiError> {
    let class_id = if let Some(selector) = request.class_selector()? {
        let target = selector.resolve_class_target(pool).await?;
        can!(
            pool,
            principal,
            scopes,
            [Permissions::ReadClass],
            target.class()
        );
        Some(crate::models::HubuumClassID::new(target.class().id)?)
    } else {
        None
    };
    let fingerprint = request.fingerprint(class_id, principal.id(), token_id, token_revision)?;
    let cursor_budget = request.reusable_cursor_budget()?;
    let page_cursor = decode_structured_search_cursor(request.cursor.as_deref(), &fingerprint)?;
    let query_options = request.query_options(class_id, page_cursor)?;
    let kind = request.target.kind();
    let authorization = StructuredAuthorizationContext {
        pool,
        principal,
        scopes,
        include_total: request.include_total,
    };
    let page_context = StructuredSearchPageContext {
        query_options: &query_options,
        fingerprint: &fingerprint,
        cursor_budget,
        version: request.version,
        kind,
    };

    match kind {
        StructuredSearchResourceKind::Collection => {
            let (rows, total) = if pool.permission_backend().supports_sql_visibility_pushdown() {
                let total = if request.include_total {
                    Some(
                        principal
                            .count_structured_collections(
                                pool,
                                count_query_options(&query_options),
                                request.filter.as_ref(),
                                scopes,
                            )
                            .await?,
                    )
                } else {
                    None
                };
                let prepared = prepare_db_pagination::<crate::models::Collection>(&query_options)?;
                let rows = principal
                    .search_structured_collections(pool, prepared, request.filter.as_ref(), scopes)
                    .await?;
                (rows, total)
            } else if !scope_allows(scopes, &[Permissions::ReadCollection]) {
                (Vec::new(), request.include_total.then_some(0))
            } else {
                let mut candidate_query = count_query_options(&query_options);
                candidate_query.limit = Some(MAX_STRUCTURED_SEARCH_EXTERNAL_CANDIDATES + 1);
                let candidates = principal
                    .search_collections_from_backend_with_admin_status_and_expression(
                        pool.db_pool(),
                        candidate_query,
                        true,
                        None,
                        request.filter.as_ref(),
                    )
                    .await?;
                authorize_structured_candidates(
                    authorization,
                    candidates,
                    &query_options,
                    vec![Permissions::ReadCollection],
                    |collection| ResourceRef::collection(collection.id),
                )
                .await?
            };
            finalize_structured_search(
                rows,
                total,
                page_context,
                StructuredSearchResult::Collection,
            )
        }
        StructuredSearchResourceKind::Class => {
            let (rows, total) = if pool.permission_backend().supports_sql_visibility_pushdown() {
                let total = if request.include_total {
                    Some(
                        principal
                            .count_structured_classes(
                                pool,
                                count_query_options(&query_options),
                                request.filter.as_ref(),
                                scopes,
                            )
                            .await?,
                    )
                } else {
                    None
                };
                let prepared =
                    prepare_db_pagination::<crate::models::HubuumClassExpanded>(&query_options)?;
                let rows = principal
                    .search_structured_classes(pool, prepared, request.filter.as_ref(), scopes)
                    .await?;
                (rows, total)
            } else if !scope_allows(
                scopes,
                &[Permissions::ReadClass, Permissions::ReadCollection],
            ) {
                (Vec::new(), request.include_total.then_some(0))
            } else {
                let mut candidate_query = count_query_options(&query_options);
                candidate_query.limit = Some(MAX_STRUCTURED_SEARCH_EXTERNAL_CANDIDATES + 1);
                let candidates = principal
                    .search_classes_from_backend_with_admin_status_and_expression(
                        pool.db_pool(),
                        candidate_query,
                        true,
                        None,
                        request.filter.as_ref(),
                    )
                    .await?;
                authorize_structured_candidates(
                    authorization,
                    candidates,
                    &query_options,
                    vec![Permissions::ReadClass, Permissions::ReadCollection],
                    |class| ResourceRef {
                        kind: ResourceKind::Class,
                        id: class.id,
                        attrs: ResourceAttrs {
                            collection_id: Some(class.collection.id),
                            name: Some(class.name.clone()),
                            ..Default::default()
                        },
                    },
                )
                .await?
            };
            finalize_structured_search(rows, total, page_context, StructuredSearchResult::Class)
        }
        StructuredSearchResourceKind::Object => {
            let (rows, total) = if pool.permission_backend().supports_sql_visibility_pushdown() {
                let total = if request.include_total {
                    Some(
                        principal
                            .count_structured_objects(
                                pool,
                                count_query_options(&query_options),
                                request.filter.as_ref(),
                                scopes,
                            )
                            .await?,
                    )
                } else {
                    None
                };
                let prepared =
                    prepare_db_pagination::<crate::models::HubuumObject>(&query_options)?;
                let rows = principal
                    .search_structured_objects(pool, prepared, request.filter.as_ref(), scopes)
                    .await?;
                (rows, total)
            } else {
                let policy_principal = PrincipalRef::load(pool, principal).await?;
                let authorization = ExternalRelatedFilterAuthorization::new(
                    pool.db_pool(),
                    pool.permission_backend(),
                    &policy_principal,
                    scopes,
                );
                let matched = externally_authorized_structured_objects(
                    principal,
                    count_query_options(&query_options),
                    request.filter.as_ref(),
                    authorization,
                )
                .await?;
                let total = request
                    .include_total
                    .then(|| i64::try_from(matched.len()))
                    .transpose()
                    .map_err(|_| {
                        ApiError::InternalServerError(
                            "Structured search result count overflow".to_string(),
                        )
                    })?;
                let prepared =
                    prepare_db_pagination::<crate::models::HubuumObject>(&query_options)?;
                let rows = paginate_in_memory(matched, &prepared)?;
                (rows, total)
            };
            finalize_structured_search(rows, total, page_context, StructuredSearchResult::Object)
        }
        StructuredSearchResourceKind::AuditEvent => {
            let (accessible_collection_ids, include_collection_less) =
                visible_event_scope(pool, principal, scopes).await?;
            let prepared = prepare_db_pagination::<crate::events::EventResponse>(&query_options)?;
            let (rows, total) = list_structured_events_with_total_count(
                pool.db_pool(),
                &accessible_collection_ids,
                include_collection_less,
                &prepared,
                request.filter.as_ref(),
            )
            .await?;
            finalize_structured_search(
                rows,
                request.include_total.then_some(total),
                page_context,
                |event| StructuredSearchResult::AuditEvent(Box::new(event)),
            )
        }
        StructuredSearchResourceKind::User => {
            let user = structured_iam_user(pool, principal, scopes, kind).await?;
            let policy_principal = PrincipalRef::load(pool, principal).await?;
            if !pool
                .permission_backend()
                .is_admin(&policy_principal)
                .await?
            {
                return Err(ApiError::Forbidden(
                    "user search requires administrator access".to_string(),
                ));
            }
            let total = if request.include_total {
                Some(
                    user.count_structured_users(
                        pool.db_pool(),
                        count_query_options(&query_options),
                        request.filter.as_ref(),
                    )
                    .await?,
                )
            } else {
                None
            };
            let prepared = prepare_db_pagination::<UserWithName>(&query_options)?;
            let rows = user
                .search_structured_users(pool.db_pool(), prepared, request.filter.as_ref())
                .await?;
            finalize_structured_search(rows, total, page_context, |user| {
                StructuredSearchResult::User(UserResponse::from(user))
            })
        }
        StructuredSearchResourceKind::Group => {
            let user = structured_iam_user(pool, principal, scopes, kind).await?;
            let total = if request.include_total {
                Some(
                    user.count_structured_groups(
                        pool.db_pool(),
                        count_query_options(&query_options),
                        request.filter.as_ref(),
                    )
                    .await?,
                )
            } else {
                None
            };
            let prepared = prepare_db_pagination::<Group>(&query_options)?;
            let rows = user
                .search_structured_groups(pool.db_pool(), prepared, request.filter.as_ref())
                .await?;
            let rows = GroupResponse::from_groups(pool, rows).await?;
            finalize_structured_search(rows, total, page_context, StructuredSearchResult::Group)
        }
        StructuredSearchResourceKind::ServiceAccount => {
            let user = structured_iam_user(pool, principal, scopes, kind).await?;
            let is_admin = user.is_admin(pool.db_pool()).await?;
            let total = if request.include_total {
                Some(
                    count_structured_manageable_service_accounts(
                        pool.db_pool(),
                        &user,
                        is_admin,
                        count_query_options(&query_options),
                        request.filter.as_ref(),
                    )
                    .await?,
                )
            } else {
                None
            };
            let prepared = prepare_db_pagination::<ServiceAccountWithName>(&query_options)?;
            let rows = search_structured_manageable_service_accounts(
                pool.db_pool(),
                &user,
                is_admin,
                prepared,
                request.filter.as_ref(),
            )
            .await?;
            finalize_structured_search(rows, total, page_context, |account| {
                StructuredSearchResult::ServiceAccount(ServiceAccountResponse::from(account))
            })
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/search",
    tag = "search",
    description = "Runs the versioned, typed Hubuum resource-search DSL. Version 1 targets collections, classes, objects, audit events, users, groups, or service accounts. Boolean and/or/not expressions compose target-specific field predicates; object queries may additionally use permission-aware existential related-object predicates and an optional exact class selector. See docs/search_api.md for the complete grammar and field/operator matrix.",
    security(("bearer_auth" = [])),
    request_body(content = StructuredSearchRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Tagged, cursor-paginated structured resource search results", body = StructuredSearchResponse),
        (status = 400, description = "Invalid DSL, predicate, or cursor", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "Explicit object target class not found", body = ApiErrorResponse),
        (status = 413, description = "Request body exceeds the structured-search size limit", body = ApiErrorResponse),
        (status = 415, description = "Content type is not application/json", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn post_search(
    pool: AppContext,
    requestor: Authenticated,
    payload: StructuredSearchPayload,
) -> Result<impl Responder, ApiError> {
    let execution = execute_structured_search(
        &pool,
        &requestor.principal,
        requestor.token_meta.id,
        requestor.token_meta.revision.get(),
        requestor.scopes(),
        payload.into_inner(),
    )
    .await?;
    let mut headers = HashMap::from([
        (
            PAGE_LIMIT_HEADER.to_string(),
            execution.page_limit.to_string(),
        ),
        ("Cache-Control".to_string(), "private, no-store".to_string()),
    ]);
    if let Some(next) = &execution.response.next {
        headers.insert(NEXT_CURSOR_HEADER.to_string(), next.clone());
    }
    if let Some(total) = execution.response.total {
        headers.insert(TOTAL_COUNT_HEADER.to_string(), total.to_string());
    }
    Ok(ApiResponse::new_with_headers(
        execution.response,
        StatusCode::OK,
        headers,
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/search/stream",
    tag = "search",
    description = "Runs the same versioned structured resource-search DSL as POST /api/v1/search and returns server-sent events. The stream emits started, zero or more tagged result events, and one terminal done event carrying cursor metadata; execution failures after streaming starts produce a terminal error event.",
    security(("bearer_auth" = [])),
    request_body(content = StructuredSearchRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Structured search server-sent event stream", content_type = "text/event-stream"),
        (status = 400, description = "Invalid DSL or request envelope", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 413, description = "Request body exceeds the structured-search size limit", body = ApiErrorResponse),
        (status = 415, description = "Content type is not application/json", body = ApiErrorResponse)
    )
)]
#[post("/stream")]
pub async fn post_stream_search(
    pool: AppContext,
    requestor: Authenticated,
    payload: StructuredSearchPayload,
) -> Result<HttpResponse, ApiError> {
    let request = payload.into_inner();
    let version = request.version;
    let kind = request.target.kind();
    let principal = requestor.principal;
    let token_id = requestor.token_meta.id;
    let token_revision = requestor.token_meta.revision.get();
    let scopes = requestor.scope;
    let execution = async move {
        execute_structured_search(
            &pool,
            &principal,
            token_id,
            token_revision,
            scopes.as_ref(),
            request,
        )
        .await
    }
    .boxed_local();
    let stream = structured_search_event_stream(version, kind, execution);

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream; charset=utf-8"))
        .insert_header(("Cache-Control", "private, no-store"))
        .insert_header(("X-Accel-Buffering", "no"))
        .streaming(stream))
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
    pool: AppContext,
    requestor: Authenticated,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let params = parse_unified_search_query(req.query_string())?;
    let limit_per_kind = params.limit_per_kind;
    let stream = execute_unified_search_stream(pool, requestor.principal, requestor.scope, params);

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

    fn empty_structured_execution() -> StructuredSearchExecution {
        StructuredSearchExecution {
            response: StructuredSearchResponse {
                version: 1,
                kind: StructuredSearchResourceKind::Object,
                results: vec![],
                next: Some("next-page".to_string()),
                total: Some(0),
            },
            page_limit: 25,
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

    #[actix_web::test]
    async fn structured_stream_emits_started_before_execution_completes() {
        let (release, blocked) = oneshot::channel::<()>();
        let execution = async move {
            blocked.await.unwrap();
            Ok(empty_structured_execution())
        }
        .boxed_local();
        let events =
            structured_search_event_stream(1, StructuredSearchResourceKind::Object, execution);
        pin_mut!(events);

        let started = events.next().await.unwrap().unwrap();
        let started = String::from_utf8(started.to_vec()).unwrap();
        assert!(started.contains("event: started"));
        assert!(started.contains("\"kind\":\"object\""));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), events.next())
                .await
                .is_err()
        );

        release.send(()).unwrap();
        let done = events.next().await.unwrap().unwrap();
        let done = String::from_utf8(done.to_vec()).unwrap();
        assert!(done.contains("event: done"));
        assert!(done.contains("\"next\":\"next-page\""));
        assert!(done.contains("\"page_limit\":25"));
        assert!(events.next().await.is_none());
    }

    #[actix_web::test]
    async fn structured_stream_error_is_terminal() {
        let execution = async {
            Err(ApiError::BadRequest(
                "structured search deliberately failed".to_string(),
            ))
        }
        .boxed_local();
        let events =
            structured_search_event_stream(1, StructuredSearchResourceKind::Collection, execution);
        pin_mut!(events);

        events.next().await.unwrap().unwrap();
        let error = events.next().await.unwrap().unwrap();
        let error = String::from_utf8(error.to_vec()).unwrap();
        assert!(error.contains("event: error"));
        assert!(error.contains("structured search deliberately failed"));
        assert!(events.next().await.is_none());
    }

    #[actix_web::test]
    async fn dropping_structured_stream_drops_pending_execution() {
        let (notify_drop, drop_observed) = oneshot::channel();
        let execution = async move {
            let _drop_notifier = DropNotifier(Some(notify_drop));
            future::pending::<Result<StructuredSearchExecution, ApiError>>().await
        }
        .boxed_local();
        let mut events = Box::pin(structured_search_event_stream(
            1,
            StructuredSearchResourceKind::Object,
            execution,
        ));

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
