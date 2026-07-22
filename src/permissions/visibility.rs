use std::time::Instant;

use crate::db::traits::authz::scope_allows_resource;
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{Permissions, TokenScope};
use crate::pagination::{known_count_or_skipped, paginate_in_memory};
use crate::traits::CursorPaginated;

use super::backend::PermissionBackend;
use super::observability::record_paginate_authorized;
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};

/// A page of authorized rows plus the total authorized count.
///
/// Constructed only by `paginate_authorized`, which today is called from
/// the Treetop backend's reverse queries. The Local backend uses the SQL
/// join fast path instead. Marked `dead_code`-allow because a build without
/// the optional Treetop backend has no caller for either type, and the lints
/// would otherwise fire.
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

    pub(crate) fn as_slice(&self) -> &[i32] {
        &self.0
    }
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
        .map(|candidate| PermissionRequest {
            resource: candidate.resource.clone(),
            permissions: permissions.to_vec(),
        })
        .collect()
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
    let requests = permission_requests(&candidates, &permissions);
    let decisions = backend.authorize_many(principal, requests).await?;
    Ok(candidates
        .into_iter()
        .zip(decisions)
        .filter_map(|(candidate, decision)| {
            (decision == PermissionDecision::Allow).then_some(candidate.value)
        })
        .collect())
}

pub async fn authorize_cursor_page<T, F>(
    backend: &dyn PermissionBackend,
    principal: &PrincipalRef,
    candidates: Vec<T>,
    scope: Option<&TokenScope>,
    permissions: Vec<Permissions>,
    query_options: &QueryOptions,
    to_resource: F,
) -> Result<AuthorizedPage<T>, ApiError>
where
    T: CursorPaginated,
    F: Fn(&T) -> ResourceRef,
{
    let start = Instant::now();
    let backend_kind = backend.kind();
    let candidate_count = candidates.len();
    let candidates = resource_scoped_candidates(candidates, scope, &to_resource);
    let requests = permission_requests(&candidates, &permissions);
    let decisions = backend.authorize_many(principal, requests).await?;
    let authorized = candidates
        .into_iter()
        .zip(decisions)
        .filter_map(|(candidate, decision)| {
            (decision == PermissionDecision::Allow).then_some(candidate.value)
        })
        .collect::<Vec<_>>();
    let authorized_count = authorized.len();
    let total_count = known_count_or_skipped(query_options, authorized_count as i64);
    let rows = paginate_in_memory(authorized, query_options)?;
    record_paginate_authorized(
        backend_kind,
        candidate_count,
        authorized_count,
        0,
        query_options.limit.unwrap_or(usize::MAX),
        rows.len(),
        start.elapsed(),
    );
    Ok(AuthorizedPage { rows, total_count })
}

/// Generic candidate-then-authorize visibility filter.
///
/// `candidates` is the full (already-loaded) candidate set — every row
/// the caller would have considered before applying permissions. The
/// caller is responsible for fetching this list via a SQL query that
/// applies all NON-permission filters (name, collection, JSON body,
/// etc.) but skips the `permissions`-table join.
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

    if candidates.is_empty() {
        record_paginate_authorized(backend_kind, 0, 0, offset, limit, 0, start.elapsed());
        return Ok(AuthorizedPage {
            rows: Vec::new(),
            total_count: 0,
        });
    }

    let requests = permission_requests(&candidates, &permissions);

    let decisions = backend.authorize_candidates(principal, requests).await?;

    let authorized: Vec<T> = candidates
        .into_iter()
        .zip(decisions)
        .filter_map(|(row, result)| {
            if result.decision == PermissionDecision::Allow {
                Some(row.value)
            } else {
                None
            }
        })
        .collect();

    let authorized_count = authorized.len();
    let total_count = authorized_count as i64;
    let rows: Vec<T> = authorized.into_iter().skip(offset).take(limit).collect();
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
