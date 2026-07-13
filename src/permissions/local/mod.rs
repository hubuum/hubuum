use std::time::Instant;

use crate::db::prelude::*;
use async_trait::async_trait;
use tokio::sync::OnceCell;

use crate::db::traits::collection as collection_backend;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    Collection, CollectionID, GroupPermission, Permission, Permissions, PermissionsList,
    PrincipalID,
};
use crate::traits::{AuthzSubject, PermissionController};

use super::backend::PermissionBackend;
use super::observability::{record_authorize_many, record_is_admin, record_reverse_query};
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef};

const BACKEND_KIND: &str = "local";

/// PostgreSQL-backed permission backend.
///
/// This adapter deliberately delegates SQL behavior to the existing current-main
/// permission traits and query helpers. That keeps local semantics aligned with
/// the canonical API surface instead of carrying a forked copy of the SQL code.
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

    async fn admin_group_id(&self) -> Result<Option<i32>, ApiError> {
        self.admin_group_id
            .get_or_try_init(|| async {
                use crate::schema::groups::dsl::{groupname, groups, id};

                with_connection(&self.pool, async |conn| {
                    groups
                        .filter(groupname.eq(&self.admin_groupname))
                        .select(id)
                        .first::<i32>(conn)
                        .await
                        .optional()
                })
                .await
            })
            .await
            .copied()
    }

    async fn collection_allows(
        &self,
        principal: &PrincipalRef,
        collection_id: i32,
        permissions: Vec<Permissions>,
    ) -> Result<bool, ApiError> {
        let collection = CollectionID::new(collection_id)?;
        let principal_id = PrincipalID::new(principal.user_id)?;
        collection
            .user_can_all(&self.pool, principal_id, permissions, None)
            .await
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
        let is_admin = self.is_admin(principal).await?;

        let mut decisions = Vec::with_capacity(requests.len());
        for request in requests {
            if is_admin {
                decisions.push(PermissionDecision::Allow);
                continue;
            }

            let allowed = match request.resource.kind {
                ResourceKind::System => false,
                ResourceKind::ClassRelation | ResourceKind::ObjectRelation => {
                    match (
                        request.resource.attrs.from_collection_id,
                        request.resource.attrs.to_collection_id,
                    ) {
                        (Some(from_ns_id), Some(to_ns_id)) if from_ns_id == to_ns_id => {
                            self.collection_allows(principal, from_ns_id, request.permissions)
                                .await?
                        }
                        (Some(from_ns_id), Some(to_ns_id)) => {
                            self.collection_allows(
                                principal,
                                from_ns_id,
                                request.permissions.clone(),
                            )
                            .await?
                                && self
                                    .collection_allows(principal, to_ns_id, request.permissions)
                                    .await?
                        }
                        _ => false,
                    }
                }
                _ => match request.resource.collection_id() {
                    Some(collection_id) => {
                        self.collection_allows(principal, collection_id, request.permissions)
                            .await?
                    }
                    None => false,
                },
            };

            decisions.push(if allowed {
                PermissionDecision::Allow
            } else {
                PermissionDecision::Deny
            });
        }

        let allow_count = decisions
            .iter()
            .filter(|decision| **decision == PermissionDecision::Allow)
            .count();
        let deny_count = decisions.len() - allow_count;
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

    async fn collections_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Collection>, ApiError> {
        use crate::schema::collections;
        use crate::schema::permissions::dsl::{
            collection_id, group_id, permissions as permissions_table,
        };

        let start = Instant::now();
        let principal_id = PrincipalID::new(principal.user_id)?;

        if permissions.is_empty() {
            let group_ids_subquery = AuthzSubject::group_ids_subquery(&principal_id);
            let collection_ids = with_connection(&self.pool, async |conn| {
                permissions_table
                    .filter(group_id.eq_any(group_ids_subquery))
                    .select(collection_id)
                    .distinct()
                    .load::<i32>(conn)
                    .await
            })
            .await?;
            let rows = with_connection(&self.pool, async |conn| {
                collections::table
                    .filter(collections::id.eq_any(collection_ids))
                    .load::<Collection>(conn)
                    .await
            })
            .await?;
            record_reverse_query(
                BACKEND_KIND,
                "collections_user_can",
                rows.len(),
                rows.len(),
                start.elapsed(),
            );
            return Ok(rows);
        }

        let mut collection_ids: Option<Vec<i32>> = None;
        for permission in permissions {
            let rows = collection_backend::user_can_on_any_from_backend(
                &self.pool,
                principal_id,
                *permission,
                None,
            )
            .await?;
            let mut ids = rows
                .into_iter()
                .map(|collection| collection.id)
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids.dedup();
            collection_ids = Some(match collection_ids {
                Some(existing) => existing
                    .into_iter()
                    .filter(|id| ids.binary_search(id).is_ok())
                    .collect(),
                None => ids,
            });
        }

        let collection_ids = collection_ids.unwrap_or_default();
        let rows = if collection_ids.is_empty() {
            Vec::new()
        } else {
            with_connection(&self.pool, async |conn| {
                collections::table
                    .filter(collections::id.eq_any(collection_ids))
                    .load::<Collection>(conn)
                    .await
            })
            .await?
        };
        record_reverse_query(
            BACKEND_KIND,
            "collections_user_can",
            rows.len(),
            rows.len(),
            start.elapsed(),
        );
        Ok(rows)
    }

    async fn groups_with_permissions_on(
        &self,
        collection_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        let start = Instant::now();
        let collection = CollectionID::new(collection_id)?;
        let (rows, total) = collection_backend::groups_on_paginated_with_total_count_from_backend(
            &self.pool,
            collection,
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
        collection_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        let start = Instant::now();
        let result =
            match collection_backend::group_on_from_backend(&self.pool, collection_id, group_id)
                .await
            {
                Ok(permission) => Ok(Some(permission)),
                Err(ApiError::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            };
        let result_count = result
            .as_ref()
            .map(|row| row.is_some() as usize)
            .unwrap_or(0);
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
        collection_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        CollectionID::new(collection_id)?
            .apply_permissions(&self.pool, group_id, list, replace_existing, None)
            .await
    }

    async fn revoke_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        CollectionID::new(collection_id)?
            .revoke(&self.pool, group_id, list, None)
            .await
    }

    async fn revoke_all(&self, collection_id: i32, group_id: i32) -> Result<(), ApiError> {
        CollectionID::new(collection_id)?
            .revoke_all(&self.pool, group_id, None)
            .await
    }

    fn supports_mutation(&self) -> bool {
        true
    }

    fn kind(&self) -> &'static str {
        BACKEND_KIND
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

    fn supports_sql_visibility_pushdown(&self) -> bool {
        true
    }

    fn uses_sql_permission_store(&self) -> bool {
        true
    }

    fn supports_permission_provenance(&self) -> bool {
        true
    }
}
