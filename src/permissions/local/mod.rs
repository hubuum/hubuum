pub mod queries;

use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::OnceCell;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    GroupPermission, Namespace, NamespaceID, Permission, Permissions, PermissionsList,
};

use super::backend::PermissionBackend;
use super::observability::{record_authorize_many, record_is_admin, record_reverse_query};
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef};

const BACKEND_KIND: &str = "local";

/// PostgreSQL-backed permission backend. Reads and writes go directly to
/// the local `permissions` table via Diesel; mutations are supported.
/// Selected at startup when `HUBUUM_PERMISSION_BACKEND=local` (default).
pub struct LocalPermissionBackend {
    pool: DbPool,
    admin_groupname: String,
    admin_group_id: OnceCell<Option<i32>>,
}

impl LocalPermissionBackend {
    pub fn new(pool: DbPool, admin_groupname: String) -> Self {
        Self {
            pool,
            admin_groupname,
            admin_group_id: OnceCell::new(),
        }
    }

    /// Resolve the admin group id from the database, caching the result.
    /// Returns None if no group with the configured admin_groupname exists.
    async fn admin_group_id(&self) -> Result<Option<i32>, ApiError> {
        self.admin_group_id
            .get_or_try_init(|| async {
                use crate::db::with_connection;
                use crate::schema::groups::dsl::{groupname, groups, id};
                use diesel::prelude::*;

                with_connection(&self.pool, |conn| {
                    groups
                        .filter(groupname.eq(&self.admin_groupname))
                        .select(id)
                        .first::<i32>(conn)
                        .optional()
                })
            })
            .await
            .map(|opt_ref| *opt_ref)
    }
}

#[async_trait]
impl PermissionBackend for LocalPermissionBackend {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        use super::types::ResourceKind;

        let start = Instant::now();
        let request_count = requests.len();

        // Check admin status once for this principal.
        let is_admin = self.is_admin(principal).await?;

        // SQL backend has no transport-side batch; loop sequentially.
        let mut decisions = Vec::with_capacity(requests.len());
        for request in requests {
            // Admin bypass: grant all permissions.
            if is_admin {
                decisions.push(PermissionDecision::Allow);
                continue;
            }

            let decision = match request.resource.kind {
                ResourceKind::ClassRelation | ResourceKind::ObjectRelation => {
                    // Relations span two namespaces and require permission on both.
                    // This preserves the legacy "AND across both namespaces" semantics
                    // from UserPermissions::can(..., namespaces.0, namespaces.1).
                    // Defensive: real AuthzTarget impls always populate both.
                    match (
                        request.resource.attrs.from_namespace_id,
                        request.resource.attrs.to_namespace_id,
                    ) {
                        (Some(from_ns_id), Some(to_ns_id)) if from_ns_id == to_ns_id => {
                            // Same-namespace relation: a single query suffices.
                            let allowed = queries::user_can_all_query(
                                &self.pool,
                                principal.user_id,
                                from_ns_id,
                                request.permissions,
                            )
                            .await?;
                            if allowed {
                                PermissionDecision::Allow
                            } else {
                                PermissionDecision::Deny
                            }
                        }
                        (Some(from_ns_id), Some(to_ns_id)) => {
                            // Cross-namespace: AND-check both, short-circuit on Deny.
                            let from_allowed = queries::user_can_all_query(
                                &self.pool,
                                principal.user_id,
                                from_ns_id,
                                request.permissions.clone(),
                            )
                            .await?;
                            if !from_allowed {
                                PermissionDecision::Deny
                            } else {
                                let to_allowed = queries::user_can_all_query(
                                    &self.pool,
                                    principal.user_id,
                                    to_ns_id,
                                    request.permissions,
                                )
                                .await?;
                                if to_allowed {
                                    PermissionDecision::Allow
                                } else {
                                    PermissionDecision::Deny
                                }
                            }
                        }
                        _ => PermissionDecision::Deny,
                    }
                }
                ResourceKind::System => {
                    // System-scoped resource: admin-only. We've already checked
                    // admin status above, so if we got here, deny.
                    PermissionDecision::Deny
                }
                _ => {
                    // Non-relation resources: use the single namespace_id field.
                    match request.resource.namespace_id() {
                        None => {
                            // No namespace and not System → defensive deny.
                            PermissionDecision::Deny
                        }
                        Some(namespace_id) => {
                            let allowed = queries::user_can_all_query(
                                &self.pool,
                                principal.user_id,
                                namespace_id,
                                request.permissions.to_vec(),
                            )
                            .await?;
                            if allowed {
                                PermissionDecision::Allow
                            } else {
                                PermissionDecision::Deny
                            }
                        }
                    }
                }
            };
            decisions.push(decision);
        }
        let allow_count = decisions
            .iter()
            .filter(|d| **d == PermissionDecision::Allow)
            .count();
        let deny_count = decisions.len() - allow_count;
        // Local backend has no transport-side batching, so cedar_request_count
        // matches request_count one-for-one.
        record_authorize_many(
            BACKEND_KIND,
            request_count,
            request_count,
            allow_count,
            deny_count,
            start.elapsed(),
        );
        Ok(decisions)
    }

    // `authorize` and `authorize_candidates` use the trait defaults — no
    // override needed. The defaults wrap `authorize_many`.

    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError> {
        let start = Instant::now();
        let result =
            queries::user_can_on_any_query(&self.pool, principal.user_id, permissions).await?;
        let n = result.len();
        // Local backend uses a SQL join, so candidate_count and result_count
        // are equal — the DB filtered before we saw the rows.
        record_reverse_query(
            BACKEND_KIND,
            "namespaces_user_can",
            n,
            n,
            start.elapsed(),
        );
        Ok(result)
    }

    async fn groups_with_permissions_on(
        &self,
        namespace_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        let start = Instant::now();
        let (rows, total) = queries::groups_on_paginated_with_total_count_query(
            &self.pool,
            NamespaceID(namespace_id),
            permissions_filter.to_vec(),
            page,
        )
        .await?;
        record_reverse_query(
            BACKEND_KIND,
            "groups_with_permissions_on",
            total as usize,
            rows.len(),
            start.elapsed(),
        );
        Ok((rows, total))
    }

    async fn group_permission_on(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        let start = Instant::now();
        let result = match queries::group_on_query(&self.pool, namespace_id, group_id).await {
            Ok(p) => Ok(Some(p)),
            Err(ApiError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        };
        let result_count = result.as_ref().map(|o| o.is_some() as usize).unwrap_or(0);
        record_reverse_query(
            BACKEND_KIND,
            "group_permission_on",
            1,
            result_count,
            start.elapsed(),
        );
        result
    }

    async fn apply_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        queries::apply_permissions_query(&self.pool, namespace_id, group_id, list, replace_existing)
            .await
    }

    async fn revoke_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        queries::revoke_permissions_query(&self.pool, namespace_id, group_id, list).await
    }

    async fn revoke_all(&self, namespace_id: i32, group_id: i32) -> Result<(), ApiError> {
        queries::revoke_all_query(&self.pool, namespace_id, group_id).await
    }

    fn supports_mutation(&self) -> bool {
        true
    }

    fn kind(&self) -> &'static str {
        "local"
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        let start = Instant::now();
        let allowed = match self.admin_group_id().await? {
            Some(admin_gid) => principal.group_ids.contains(&admin_gid),
            None => false,
        };
        record_is_admin(BACKEND_KIND, allowed, start.elapsed());
        Ok(allowed)
    }

    fn supports_sql_visibility_join(&self) -> bool {
        true
    }
}
