use crate::errors::ApiError;
use crate::models::collection::user_can_on_any;
use crate::models::search::QueryOptions;
use crate::models::{HistoryAuthorizationSnapshot, Permissions, TokenScope};
use crate::permissions::visibility::authorize_cursor_page_from_storage;
use crate::permissions::{AppContext, PrincipalRef, authorize_resources};
use crate::traits::{AuthzSubject, CursorPaginated, scope_allows};
use std::future::Future;

/// Authorize one historical resource shape, including its stored attributes.
pub async fn authorize_history_snapshot<S>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    permission: Permissions,
    snapshot: HistoryAuthorizationSnapshot,
) -> Result<(), ApiError>
where
    S: AuthzSubject + ?Sized,
{
    authorize_resources(
        context.permission_backend(),
        context,
        subject,
        scopes,
        vec![permission],
        vec![snapshot.into_resource()],
    )
    .await
}

/// Fetch bounded historical pages and authorize their stored resource snapshots.
pub async fn authorize_history_page<S, T, F, Fetch, FetchFuture>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    permission: Permissions,
    fetch: Fetch,
    query_options: &QueryOptions,
    to_snapshot: F,
) -> Result<(Vec<T>, i64), ApiError>
where
    S: AuthzSubject + ?Sized,
    T: CursorPaginated,
    F: Fn(&T) -> HistoryAuthorizationSnapshot,
    Fetch: FnMut(QueryOptions) -> FetchFuture,
    FetchFuture: Future<Output = Result<Vec<T>, ApiError>>,
{
    if !scope_allows(scopes, &[permission]) {
        return Err(ApiError::Forbidden("Permission denied".to_string()));
    }
    let principal = PrincipalRef::load(context, subject).await?;
    let page = authorize_cursor_page_from_storage(
        context.permission_backend(),
        &principal,
        scopes,
        vec![permission],
        query_options,
        fetch,
        |candidate| {
            to_snapshot(candidate)
                .into_resource()
                .normalized_for_permission(permission)
        },
    )
    .await?;
    Ok((page.rows, page.total_count))
}

/// Resolve the collection ids visible through the local SQL permission store.
pub async fn readable_history_collection_ids<S>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    permission: Permissions,
) -> Result<Vec<i32>, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    let mut collection_ids = user_can_on_any(context, subject, permission, scopes)
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();
    collection_ids.sort_unstable();
    collection_ids.dedup();
    Ok(collection_ids)
}

/// Deleted resources have no live row to authorize against. Only unscoped
/// admins may use the history endpoints as a compliance/audit surface for
/// those tombstones; ordinary callers must pass the normal live-resource check.
pub async fn can_read_deleted_history<S>(
    context: &AppContext,
    subject: &S,
    token_scoped: bool,
) -> Result<bool, ApiError>
where
    S: AuthzSubject + ?Sized,
{
    if token_scoped {
        return Ok(false);
    }
    let principal = PrincipalRef::load(context, subject).await?;
    context.permission_backend().is_admin(&principal).await
}
