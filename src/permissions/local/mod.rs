use std::time::Instant;

use async_trait::async_trait;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::search::QueryOptions;
use crate::models::{
    Collection, CollectionID, GroupID, GroupPermission, Permission, Permissions, PermissionsList,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::permissions::storage::{
    collection_from_storage, grant_from_storage, group_grant_from_storage, permission_to_storage,
};
use crate::services::storage_boundary::{collection_id_to_storage, principal_id_to_storage};
use crate::storage::{
    AuthorizationCollectionAccessQuery, AuthorizationCollectionGrantListQuery,
    AuthorizationCollectionsQuery, AuthorizationDataStorage, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroupMembershipQuery,
    StorageHandle,
};

use super::backend::PermissionBackend;
use super::observability::{record_authorize_many, record_is_admin, record_reverse_query};
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef};

const BACKEND_KIND: &str = "local";

/// Built-in permission backend backed by the mandatory authorization storage
/// contract.
///
/// Policy decisions and grant management use backend-neutral requests and
/// DTOs. The selected storage adapter owns its persistence and query details.
pub struct LocalPermissionBackend {
    storage: StorageHandle,
    admin_groupname: String,
}

impl LocalPermissionBackend {
    pub(crate) fn new(storage: StorageHandle, admin_groupname: String) -> Self {
        Self {
            storage,
            admin_groupname,
        }
    }

    async fn collection_allows(
        &self,
        principal: &PrincipalRef,
        collection_id: i32,
        permissions: Vec<Permissions>,
    ) -> Result<bool, ApiError> {
        let query = AuthorizationCollectionAccessQuery::new(
            principal_id_to_storage(principal.user_id),
            collection_id_to_storage(collection_id),
            permissions.into_iter().map(permission_to_storage),
        );
        Ok(self.storage.authorize_local_collection(query).await?)
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
        let start = Instant::now();
        let query = AuthorizationCollectionsQuery::new(
            principal_id_to_storage(principal.user_id),
            permissions.iter().copied().map(permission_to_storage),
        );
        let rows = self
            .storage
            .list_local_authorized_collections(query)
            .await?
            .into_iter()
            .map(collection_from_storage)
            .collect::<Result<Vec<_>, _>>()?;
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
        collection_id: CollectionID,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        let start = Instant::now();
        let query = AuthorizationCollectionGrantListQuery::new(
            collection_id,
            permissions_filter
                .iter()
                .copied()
                .map(permission_to_storage),
            page.clone(),
        );
        let (rows, total) = self
            .storage
            .list_local_collection_grants(query)
            .await?
            .into_parts();
        let rows = rows
            .into_iter()
            .map(group_grant_from_storage)
            .collect::<Result<Vec<_>, _>>()?;
        let observed_total = total
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(rows.len());
        record_reverse_query(
            BACKEND_KIND,
            "groups_with_permissions_on",
            observed_total,
            rows.len(),
            start.elapsed(),
        );
        Ok((rows, total.unwrap_or(SKIPPED_TOTAL_COUNT)))
    }

    async fn group_permission_on(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<Option<Permission>, ApiError> {
        let start = Instant::now();
        let key = AuthorizationGrantKey::new(collection_id, group_id);
        let result = self
            .storage
            .get_local_collection_grant(key)
            .await
            .map(|grant| grant.map(grant_from_storage))
            .map_err(ApiError::from);
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
        collection_id: CollectionID,
        group_id: GroupID,
        list: PermissionsList,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        let mutation = AuthorizationGrantMutation::new(
            AuthorizationGrantKey::new(collection_id, group_id),
            list.iter().copied().map(permission_to_storage),
            replace_existing,
            EventContext::system(),
        );
        Ok(grant_from_storage(
            self.storage
                .apply_local_collection_grant(mutation)
                .await?
                .into_value(),
        ))
    }

    async fn revoke_permissions(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
        list: PermissionsList,
    ) -> Result<Permission, ApiError> {
        let mutation = AuthorizationGrantMutation::new(
            AuthorizationGrantKey::new(collection_id, group_id),
            list.iter().copied().map(permission_to_storage),
            false,
            EventContext::system(),
        );
        Ok(grant_from_storage(
            self.storage
                .revoke_local_collection_grant(mutation)
                .await?
                .into_value(),
        ))
    }

    async fn revoke_all(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<(), ApiError> {
        let request = AuthorizationGrantDelete::new(
            AuthorizationGrantKey::new(collection_id, group_id),
            EventContext::system(),
        );
        self.storage
            .revoke_all_local_collection_grants(request)
            .await?
            .into_value();
        Ok(())
    }

    fn supports_mutation(&self) -> bool {
        true
    }

    fn kind(&self) -> &'static str {
        BACKEND_KIND
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        let start = Instant::now();
        let identity_scope = crate::config::get_config()?
            .admin_identity_scope
            .clone()
            .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
        let query = AuthorizationGroupMembershipQuery::new(
            principal_id_to_storage(principal.user_id),
            &self.admin_groupname,
            identity_scope,
        );
        let allowed = self
            .storage
            .is_authorization_principal_group_member(query)
            .await?;
        record_is_admin(BACKEND_KIND, allowed, start.elapsed());
        Ok(allowed)
    }

    fn supports_storage_visibility_filtering(&self) -> bool {
        true
    }

    fn uses_local_permission_store(&self) -> bool {
        true
    }

    fn supports_permission_provenance(&self) -> bool {
        true
    }
}
