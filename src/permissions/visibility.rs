use std::time::Instant;

use crate::errors::ApiError;
use crate::models::Permissions;

use super::backend::PermissionBackend;
use super::observability::record_paginate_authorized;
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};

/// A page of authorized rows plus the total authorized count.
pub struct AuthorizedPage<T> {
    pub rows: Vec<T>,
    pub total_count: i64,
}

/// Generic candidate-then-authorize visibility filter.
///
/// `candidates` is the full (already-loaded) candidate set — every row
/// the caller would have considered before applying permissions. The
/// caller is responsible for fetching this list via a SQL query that
/// applies all NON-permission filters (name, namespace, JSON body,
/// etc.) but skips the `permissions`-table join.
///
/// `to_resource` maps each candidate to the [`ResourceRef`] used for
/// authorization. `permissions` is the conjunctive permission set
/// required to make a row visible (typically a single permission like
/// `Permissions::ReadObject`).
///
/// `offset` and `limit` apply AFTER authorization filtering. The
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
    permissions: Vec<Permissions>,
    offset: usize,
    limit: usize,
    to_resource: F,
) -> Result<AuthorizedPage<T>, ApiError>
where
    F: Fn(&T) -> ResourceRef,
{
    let start = Instant::now();
    let backend_kind = backend.kind();
    let candidate_count = candidates.len();

    if candidates.is_empty() {
        record_paginate_authorized(backend_kind, 0, 0, offset, limit, 0, start.elapsed());
        return Ok(AuthorizedPage {
            rows: Vec::new(),
            total_count: 0,
        });
    }

    let requests: Vec<PermissionRequest> = candidates
        .iter()
        .map(|c| PermissionRequest {
            resource: to_resource(c),
            permissions: permissions.clone(),
        })
        .collect();

    let decisions = backend.authorize_candidates(principal, requests).await?;

    let authorized: Vec<T> = candidates
        .into_iter()
        .zip(decisions)
        .filter_map(|(row, result)| {
            if result.decision == PermissionDecision::Allow {
                Some(row)
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
