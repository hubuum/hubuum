use crate::errors::ApiError;
use crate::models::collection::user_can_on_any;
use crate::models::search::QueryOptions;
use crate::models::{HistoryAuthorizationSnapshot, Permissions, TokenScope};
use crate::permissions::visibility::{
    authorize_cursor_page_from_storage, authorize_resource_permissions,
};
use crate::permissions::{AppContext, PrincipalRef, authorize_resources};
use crate::permissions::{PermissionBackend, ResourceRef, permission_to_storage};
use crate::storage::{
    CollectionAuthorizationQueryStorage, StorageAuthorizationCollectionVisibilityQuery,
    StorageHistoryCollectionScope, storage_handle,
};
use crate::traits::{AuthzSubject, CursorPaginated, scope_allows};
use hubuum_domain::PrincipalId;
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

/// Request-local authorization facts from the configured policy backend.
/// Resource scopes stay attached to every point and candidate check.
pub(super) struct HistoryAccess<'a> {
    context: &'a AppContext,
    principal: PrincipalRef,
    scopes: Option<&'a TokenScope>,
    administrator: bool,
}

/// Only HistoryAccess can mint the unscoped deleted-history exception.
#[derive(Clone, Copy)]
pub(super) struct DeletedHistoryAccess(());

pub(super) enum HistoryIdentity<T> {
    Live(T),
    Deleted(DeletedHistoryAccess),
}

impl<'a> HistoryAccess<'a> {
    pub(super) async fn load(
        context: &'a AppContext,
        subject: &(impl AuthzSubject + ?Sized),
        scopes: Option<&'a TokenScope>,
    ) -> Result<Self, ApiError> {
        let principal = PrincipalRef::load(context, subject).await?;
        let administrator = context.permission_backend().is_admin(&principal).await?;
        Ok(Self {
            context,
            principal,
            scopes,
            administrator,
        })
    }

    pub(super) fn context(&self) -> &'a AppContext {
        self.context
    }
    pub(super) fn backend(&self) -> &'a dyn PermissionBackend {
        self.context.permission_backend()
    }
    pub(super) fn principal(&self) -> &PrincipalRef {
        &self.principal
    }
    pub(super) fn scopes(&self) -> Option<&'a TokenScope> {
        self.scopes
    }
    pub(super) fn deleted(&self) -> Option<DeletedHistoryAccess> {
        (self.scopes.is_none() && self.administrator).then_some(DeletedHistoryAccess(()))
    }
    pub(super) fn identity<T>(&self, current: Option<T>) -> Option<HistoryIdentity<T>> {
        match current {
            Some(value) => Some(HistoryIdentity::Live(value)),
            None => self.deleted().map(HistoryIdentity::Deleted),
        }
    }

    /// Coarse candidate filtering never substitutes collection membership for
    /// token resource scope. Scoped requests must still authorize each candidate.
    pub(super) async fn candidate_collections(
        &self,
        permission: Permissions,
    ) -> Result<StorageHistoryCollectionScope, ApiError> {
        if !self.backend().supports_storage_visibility_filtering() {
            return Ok(StorageHistoryCollectionScope::All);
        }
        let rows = storage_handle(self.context)
            .list_visible_collections(StorageAuthorizationCollectionVisibilityQuery::new(
                PrincipalId::new(self.principal.user_id)?,
                self.administrator,
                permission_to_storage(permission),
                None,
            ))
            .await?;
        Ok(StorageHistoryCollectionScope::Visible(
            rows.into_iter().map(|row| row.id()).collect(),
        ))
    }

    pub(super) fn uses_storage_visibility(&self) -> bool {
        self.scopes.is_none() && self.backend().supports_storage_visibility_filtering()
    }

    pub(super) async fn authorize(
        &self,
        permission: Permissions,
        resource: ResourceRef,
    ) -> Result<(), ApiError> {
        if authorize_resource_permissions(
            self.backend(),
            &self.principal,
            &resource,
            self.scopes,
            &[permission],
        )
        .await?
        {
            Ok(())
        } else {
            Err(ApiError::Forbidden("Permission denied".into()))
        }
    }

    pub(super) async fn page<T, F, Fetch, FetchFuture>(
        &self,
        permission: Permissions,
        fetch: Fetch,
        options: &QueryOptions,
        to_snapshot: F,
    ) -> Result<(Vec<T>, i64), ApiError>
    where
        T: CursorPaginated,
        F: Fn(&T) -> HistoryAuthorizationSnapshot,
        Fetch: FnMut(QueryOptions) -> FetchFuture,
        FetchFuture: Future<Output = Result<Vec<T>, ApiError>>,
    {
        if !scope_allows(self.scopes, &[permission]) {
            return Err(ApiError::Forbidden("Permission denied".into()));
        }
        let page = authorize_cursor_page_from_storage(
            self.backend(),
            &self.principal,
            self.scopes,
            vec![permission],
            options,
            fetch,
            |row| {
                to_snapshot(row)
                    .into_resource()
                    .normalized_for_permission(permission)
            },
        )
        .await?;
        Ok((page.rows, page.total_count))
    }
}
