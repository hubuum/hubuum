pub mod queries;

use async_trait::async_trait;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    GroupPermission, Namespace, NamespaceID, Permission, Permissions, PermissionsList,
};

use super::backend::PermissionBackend;
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef};

pub struct LocalPermissionBackend {
    pool: DbPool,
}

impl LocalPermissionBackend {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PermissionBackend for LocalPermissionBackend {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        // SQL backend has no transport-side batch; loop sequentially.
        let mut decisions = Vec::with_capacity(requests.len());
        for request in requests {
            let decision = match request.resource.namespace_id() {
                None => {
                    // System-scoped checks: admin short-circuit lives one
                    // level up in PermissionController. Here we deny.
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
            };
            decisions.push(decision);
        }
        Ok(decisions)
    }

    // `authorize` and `authorize_candidates` use the trait defaults — no
    // override needed. The defaults wrap `authorize_many`.

    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError> {
        queries::user_can_on_any_query(&self.pool, principal.user_id, permissions).await
    }

    async fn groups_with_permissions_on(
        &self,
        namespace_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        queries::groups_on_paginated_with_total_count_query(
            &self.pool,
            NamespaceID(namespace_id),
            permissions_filter.to_vec(),
            page,
        )
        .await
    }

    async fn group_permission_on(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        match queries::group_on_query(&self.pool, namespace_id, group_id).await {
            Ok(p) => Ok(Some(p)),
            Err(ApiError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
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
}
