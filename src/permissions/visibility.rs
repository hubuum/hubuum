use std::future::Future;
use std::time::Instant;

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{Permissions, TokenScope};
use crate::pagination::{
    CursorBoundary, effective_page_limit, item_is_after_cursor, known_count_or_skipped,
    prepare_db_pagination,
};
use crate::traits::{CursorPaginated, scope_allows, scope_allows_resource};

use super::backend::PermissionBackend;
use super::observability::record_paginate_authorized;
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};

/// Bound duplicate request/resource allocations while walking a candidate set.
/// Backends may apply a smaller wire-level limit of their own.
const MAX_AUTHORIZATION_CHECKS_PER_BATCH: usize = 512;
const STORAGE_AUTHORIZATION_CANDIDATE_PAGE_SIZE: usize = 128;

/// A page of authorized rows plus the total authorized count.
///
/// Constructed by the candidate-authorization visibility helpers. The Local
/// backend normally uses its SQL join fast path instead.
pub struct AuthorizedPage<T> {
    pub rows: Vec<T>,
    pub total_count: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorizationPage {
    offset: usize,
    limit: usize,
}

impl AuthorizationPage {
    pub const fn new(offset: usize, limit: usize) -> Self {
        Self { offset, limit }
    }
}

/// Sorted, deduplicated object ids that have already passed policy authorization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizedObjectIds(Vec<i32>);

impl AuthorizedObjectIds {
    pub(crate) const fn empty() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn new(ids: impl IntoIterator<Item = i32>) -> Result<Self, ApiError> {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        if ids.iter().any(|id| *id <= 0) {
            return Err(ApiError::InternalServerError(
                "Authorized object ids must be positive".to_string(),
            ));
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(Self(ids))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn contains(&self, object_id: i32) -> bool {
        self.0.binary_search(&object_id).is_ok()
    }

    pub(crate) fn as_slice(&self) -> &[i32] {
        &self.0
    }
}

/// Authorize a conjunctive permission set on one resource.
///
/// Resource scope is evaluated against the concrete resource before each
/// permission is normalized to the resource kind expected by the policy
/// schema. This supports checks such as `ReadClass + ReadCollection` without
/// losing a class-scoped token boundary.
pub(crate) async fn authorize_resource_permissions(
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    resource: &ResourceRef,
    scope: Option<&TokenScope>,
    permissions: &[Permissions],
) -> Result<bool, ApiError> {
    if !scope_allows(scope, permissions) || !scope_allows_resource(scope, resource) {
        return Ok(false);
    }

    let requests = permissions
        .iter()
        .map(|permission| PermissionRequest {
            resource: resource.normalized_for_permission(*permission),
            permissions: vec![*permission],
        })
        .collect::<Vec<_>>();
    let expected_decisions = requests.len();
    let decisions = backend.authorize_many(principal, requests).await?;
    if decisions.len() != expected_decisions {
        return Err(ApiError::InternalServerError(
            "Permission backend returned an unexpected number of decisions".to_string(),
        ));
    }

    Ok(decisions
        .into_iter()
        .all(|decision| decision == PermissionDecision::Allow))
}

struct ResourceScopedCandidate<T> {
    value: T,
    resource: ResourceRef,
}

fn resource_scoped_candidates<T, F>(
    candidates: Vec<T>,
    scope: Option<&TokenScope>,
    to_resource: &F,
) -> Vec<ResourceScopedCandidate<T>>
where
    F: Fn(&T) -> ResourceRef,
{
    candidates
        .into_iter()
        .filter_map(|value| {
            let resource = to_resource(&value);
            scope_allows_resource(scope, &resource)
                .then_some(ResourceScopedCandidate { value, resource })
        })
        .collect()
}

fn permission_requests<T>(
    candidates: &[ResourceScopedCandidate<T>],
    permissions: &[Permissions],
) -> Vec<PermissionRequest> {
    candidates
        .iter()
        .flat_map(|candidate| {
            permissions.iter().map(|permission| PermissionRequest {
                resource: candidate.resource.normalized_for_permission(*permission),
                permissions: vec![*permission],
            })
        })
        .collect()
}

fn candidate_batch_size(permissions: &[Permissions]) -> usize {
    (MAX_AUTHORIZATION_CHECKS_PER_BATCH / permissions.len().max(1)).max(1)
}

fn allowed_candidate_values<T>(
    candidates: Vec<ResourceScopedCandidate<T>>,
    decisions: Vec<PermissionDecision>,
    permission_count: usize,
) -> Result<Vec<T>, ApiError> {
    let expected_decisions = candidates.len().saturating_mul(permission_count);
    if decisions.len() != expected_decisions {
        return Err(ApiError::InternalServerError(
            "Permission backend returned an unexpected number of decisions".to_string(),
        ));
    }
    if permission_count == 0 {
        return Ok(candidates
            .into_iter()
            .map(|candidate| candidate.value)
            .collect());
    }
    Ok(candidates
        .into_iter()
        .zip(decisions.chunks_exact(permission_count))
        .filter_map(|(candidate, candidate_decisions)| {
            candidate_decisions
                .iter()
                .all(|decision| *decision == PermissionDecision::Allow)
                .then_some(candidate.value)
        })
        .collect())
}

async fn visit_authorized_candidates<T, F>(
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    candidates: Vec<ResourceScopedCandidate<T>>,
    permissions: &[Permissions],
    mut visit: F,
) -> Result<usize, ApiError>
where
    F: FnMut(T),
{
    let mut candidates = candidates.into_iter();
    let mut authorized_count = 0;
    let batch_size = candidate_batch_size(permissions);

    loop {
        let batch = candidates.by_ref().take(batch_size).collect::<Vec<_>>();
        if batch.is_empty() {
            break;
        }

        let requests = permission_requests(&batch, permissions);
        let decisions = backend.authorize_many(principal, requests).await?;
        let allowed = allowed_candidate_values(batch, decisions, permissions.len())?;
        authorized_count += allowed.len();
        allowed.into_iter().for_each(&mut visit);
    }

    Ok(authorized_count)
}

pub async fn authorize_all_candidates<T, F>(
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    candidates: Vec<T>,
    scope: Option<&TokenScope>,
    permissions: Vec<Permissions>,
    to_resource: F,
) -> Result<Vec<T>, ApiError>
where
    F: Fn(&T) -> ResourceRef,
{
    let candidates = resource_scoped_candidates(candidates, scope, &to_resource);
    let mut authorized = Vec::new();
    visit_authorized_candidates(backend, principal, candidates, &permissions, |candidate| {
        authorized.push(candidate)
    })
    .await?;
    Ok(authorized)
}

/// Authorize a storage-backed candidate enumeration without materializing the
/// complete pre-authorization result set.
///
/// The fetcher receives a normalized, cursor-paged query whose limit includes
/// one look-ahead row. When an exact total is requested every candidate page is
/// visited, but only the requested authorized page is retained. When totals are
/// skipped, enumeration stops as soon as one response page plus look-ahead has
/// been authorized.
/// Storage ordering and cursor predicates must follow `CursorValue` ordering,
/// including bytewise string comparisons, so exact totals can locate the
/// response boundary while scanning from the beginning.
pub async fn authorize_cursor_page_from_storage<T, Fetch, FetchFuture, ToResource>(
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    scope: Option<&TokenScope>,
    permissions: Vec<Permissions>,
    query_options: &QueryOptions,
    fetch: Fetch,
    to_resource: ToResource,
) -> Result<AuthorizedPage<T>, ApiError>
where
    T: CursorPaginated,
    Fetch: FnMut(QueryOptions) -> FetchFuture,
    FetchFuture: Future<Output = Result<Vec<T>, ApiError>>,
    ToResource: Fn(&T) -> ResourceRef,
{
    filter_authorized_cursor_page_from_storage(backend, query_options, fetch, |candidates| {
        authorize_all_candidates(
            backend,
            principal,
            candidates,
            scope,
            permissions.clone(),
            &to_resource,
        )
    })
    .await
}

/// Enumerate raw storage pages, then apply an order-preserving authorization
/// filter. Filtering must happen here, after the storage continuation is captured:
/// a page containing only denied rows must not be mistaken for storage exhaustion.
/// The filter may resolve bounded relation metadata or evaluate related predicates.
pub(crate) async fn filter_authorized_cursor_page_from_storage<
    T,
    Fetch,
    FetchFuture,
    Filter,
    FilterFuture,
>(
    backend: &dyn PermissionBackend,
    query_options: &QueryOptions,
    mut fetch: Fetch,
    mut filter: Filter,
) -> Result<AuthorizedPage<T>, ApiError>
where
    T: CursorPaginated,
    Fetch: FnMut(QueryOptions) -> FetchFuture,
    FetchFuture: Future<Output = Result<Vec<T>, ApiError>>,
    Filter: FnMut(Vec<T>) -> FilterFuture,
    FilterFuture: Future<Output = Result<Vec<T>, ApiError>>,
{
    let start = Instant::now();
    let backend_kind = backend.kind();
    let response_limit = effective_page_limit(query_options)?.saturating_add(1);
    // Validate the caller's cursor even when exact totals restart enumeration
    // before it, including queries whose candidate set is empty.
    let mut candidate_query = prepare_db_pagination::<T>(query_options)?;
    if query_options.include_total() {
        candidate_query.clear_cursor();
    }
    candidate_query.set_include_total(false);

    let mut rows = Vec::with_capacity(response_limit);
    let mut candidate_count = 0_usize;
    let mut authorized_count = 0_usize;

    loop {
        let candidate_limit = if query_options.include_total() {
            STORAGE_AUTHORIZATION_CANDIDATE_PAGE_SIZE
        } else {
            STORAGE_AUTHORIZATION_CANDIDATE_PAGE_SIZE.min(response_limit - rows.len())
        };
        let maximum_rows = candidate_limit.saturating_add(1);
        candidate_query.set_limit(Some(maximum_rows))?;
        let mut candidates = fetch(candidate_query.clone()).await?;
        record_candidate_fetch(candidates.len());
        if candidates.len() > maximum_rows {
            return Err(ApiError::InternalServerError(format!(
                "Storage returned {} authorization candidates for a page limited to {maximum_rows}",
                candidates.len()
            )));
        }
        let has_more = candidates.len() > candidate_limit;
        if has_more {
            candidates.truncate(candidate_limit);
        }
        let next_boundary = if has_more {
            candidates
                .last()
                .map(|candidate| CursorBoundary::from_item(candidate, candidate_query.sort()))
        } else {
            None
        };
        candidate_count = candidate_count.saturating_add(candidates.len());

        let authorized = filter(candidates).await?;
        authorized_count = authorized_count.saturating_add(authorized.len());
        for candidate in authorized {
            let belongs_to_response = query_options
                .cursor()
                .map(|cursor| item_is_after_cursor(&candidate, cursor, candidate_query.sort()))
                .transpose()?
                .unwrap_or(true);
            if belongs_to_response && rows.len() < response_limit {
                rows.push(candidate);
            }
        }

        if (!query_options.include_total() && rows.len() >= response_limit) || !has_more {
            break;
        }
        candidate_query.set_cursor(
            next_boundary
                .transpose()?
                .map(CursorBoundary::encode)
                .transpose()?,
        )?;
    }

    let total_count = known_count_or_skipped(query_options, authorized_count as i64);
    record_paginate_authorized(
        backend_kind,
        candidate_count,
        authorized_count,
        0,
        response_limit.saturating_sub(1),
        rows.len(),
        start.elapsed(),
    );
    Ok(AuthorizedPage { rows, total_count })
}

fn record_candidate_fetch(rows: usize) {
    tracing::debug!(target: "hubuum::permissions", fetched_rows = rows, "authorization_candidate_page");
    #[cfg(test)]
    let _ = CANDIDATE_FETCHES.try_with(|fetches| fetches.borrow_mut().push(rows));
}

#[cfg(test)]
tokio::task_local! {
    static CANDIDATE_FETCHES: std::cell::RefCell<Vec<usize>>;
}

#[cfg(test)]
pub(crate) async fn capture_candidate_fetches<T>(
    future: impl Future<Output = T>,
) -> (T, Vec<usize>) {
    CANDIDATE_FETCHES
        .scope(std::cell::RefCell::new(Vec::new()), async {
            let result = future.await;
            (
                result,
                CANDIDATE_FETCHES.with(|fetches| fetches.borrow().clone()),
            )
        })
        .await
}

/// Generic candidate-then-authorize visibility filter.
///
/// `candidates` is the full (already-loaded) candidate set — every row
/// the caller would have considered before applying permissions. The
/// caller is responsible for fetching this list via a SQL query that
/// applies all NON-permission filters (name, collection, JSON body,
/// etc.) but skips the `permissions`-table join.
/// Authorization requests are assembled and dispatched in bounded batches;
/// the candidate vector itself remains the caller's responsibility.
///
/// `to_resource` maps each candidate to the [`ResourceRef`] used for
/// authorization. `permissions` is the conjunctive permission set
/// required to make a row visible (typically a single permission like
/// `Permissions::ReadObject`).
///
/// `page` applies its offset and limit AFTER authorization filtering. The
/// returned `total_count` is the count of authorized rows, NOT the
/// candidate set count — so paging works correctly under Treetop.
///
/// Pagination shape: caller provides offset/limit because cursor
/// semantics live a layer up; this helper concerns itself only with
/// the authorize-then-page pipeline. The candidate set must already
/// be sorted in the order the caller wants pagination to apply.
pub async fn paginate_authorized<T, F>(
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    candidates: Vec<T>,
    scope: Option<&TokenScope>,
    permissions: Vec<Permissions>,
    page: AuthorizationPage,
    to_resource: F,
) -> Result<AuthorizedPage<T>, ApiError>
where
    F: Fn(&T) -> ResourceRef,
{
    let AuthorizationPage { offset, limit } = page;
    let start = Instant::now();
    let backend_kind = backend.kind();
    let candidate_count = candidates.len();
    let candidates = resource_scoped_candidates(candidates, scope, &to_resource);
    let mut rows = Vec::new();
    let mut authorized_seen = 0;
    let authorized_count =
        visit_authorized_candidates(backend, principal, candidates, &permissions, |candidate| {
            if authorized_seen >= offset && rows.len() < limit {
                rows.push(candidate);
            }
            authorized_seen += 1;
        })
        .await?;
    let total_count = authorized_count as i64;
    let returned_count = rows.len();

    record_paginate_authorized(
        backend_kind,
        candidate_count,
        authorized_count,
        offset,
        limit,
        returned_count,
        start.elapsed(),
    );

    Ok(AuthorizedPage { rows, total_count })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_decision_count_mismatch_fails_closed() {
        let candidates = vec![ResourceScopedCandidate {
            value: 7,
            resource: ResourceRef::collection(7),
        }];

        let error = allowed_candidate_values(candidates, Vec::new(), 1).unwrap_err();

        assert_eq!(
            error,
            ApiError::InternalServerError(
                "Permission backend returned an unexpected number of decisions".to_string()
            )
        );
    }
}
