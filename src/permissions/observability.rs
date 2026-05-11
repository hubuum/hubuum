//! Tracing helpers for the permission backend boundary.
//!
//! Every permission-backend method that crosses an interesting boundary
//! (a Cedar request, a SQL query, a candidate-set walk) emits a single
//! structured event through one of the helpers here. Centralizing the
//! emit keeps the event field names stable across backends so operators
//! can filter and aggregate the same way for both `local` and `treetop`.
//!
//! All events use the `hubuum::permissions` target so a single
//! `RUST_LOG=hubuum::permissions=debug` toggle lights them up without
//! pulling in unrelated debug spam.
//!
//! Field discipline: we deliberately do NOT log policy payloads, full
//! `ResourceAttrs`, or principal group lists. Counts and durations only.
//! If you need to debug a specific request, raise the level on the SQL
//! crate or the underlying treetop-client — not on this module.
//!
//! Latency is measured around the boundary call and reported in
//! milliseconds (`u64`). For queries that finish in well under a
//! millisecond this rounds to 0; if sub-ms timing matters we can add an
//! `_us` field, but the operational signal we care about — slow
//! authorize batches, slow reverse queries — is well above that floor.

use std::time::Duration;

use tracing::debug;

const TARGET: &str = "hubuum::permissions";

/// Emit an event for `PermissionBackend::authorize_many`.
///
/// `cedar_request_count` is the number of requests sent to the underlying
/// engine — for Treetop this is the expanded batch size (one Cedar request
/// per Permission per PermissionRequest), for Local it's the same as
/// `request_count` because the SQL backend has no batch transport.
pub fn record_authorize_many(
    backend: &'static str,
    request_count: usize,
    cedar_request_count: usize,
    allow_count: usize,
    deny_count: usize,
    elapsed: Duration,
) {
    debug!(
        target: TARGET,
        backend,
        request_count,
        cedar_request_count,
        allow_count,
        deny_count,
        latency_ms = elapsed.as_millis() as u64,
        "authorize_many"
    );
}

/// Emit an event for `PermissionBackend::is_admin`.
pub fn record_is_admin(backend: &'static str, allowed: bool, elapsed: Duration) {
    debug!(
        target: TARGET,
        backend,
        allowed,
        latency_ms = elapsed.as_millis() as u64,
        "is_admin"
    );
}

/// Emit an event for a reverse query
/// (`namespaces_user_can`, `groups_with_permissions_on`,
/// `group_permission_on`).
///
/// `query` is a short identifier for the method ("namespaces_user_can",
/// etc.) so operators can filter events for a single reverse query
/// without parsing the message field.
///
/// `candidate_count` is the size of the candidate set the backend
/// considered (e.g. all namespaces in the DB). `result_count` is the
/// number of rows the backend returned to the caller. For a
/// candidate-then-authorize backend (Treetop) the difference between
/// the two reflects how much Cedar pruned; for the SQL backend
/// `candidate_count` and `result_count` will typically match because
/// the join already filters.
pub fn record_reverse_query(
    backend: &'static str,
    query: &'static str,
    candidate_count: usize,
    result_count: usize,
    elapsed: Duration,
) {
    debug!(
        target: TARGET,
        backend,
        query,
        candidate_count,
        result_count,
        latency_ms = elapsed.as_millis() as u64,
        "reverse_query"
    );
}

/// Emit an event for the `paginate_authorized` helper.
///
/// `authorized_count` is the size of the authorized set BEFORE
/// pagination is applied; `returned_count` is the number of rows that
/// actually survived `offset`/`limit`. Both are interesting: the first
/// tells you how much your candidate query filtered down to, the second
/// tells you what the API actually shipped.
pub fn record_paginate_authorized(
    backend: &'static str,
    candidate_count: usize,
    authorized_count: usize,
    offset: usize,
    limit: usize,
    returned_count: usize,
    elapsed: Duration,
) {
    debug!(
        target: TARGET,
        backend,
        candidate_count,
        authorized_count,
        offset,
        limit,
        returned_count,
        latency_ms = elapsed.as_millis() as u64,
        "paginate_authorized"
    );
}
