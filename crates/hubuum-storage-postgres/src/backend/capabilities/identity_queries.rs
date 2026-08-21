use super::super::*;

#[async_trait]
impl GroupStorage for PostgresStorage {
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        crate::operations::group::list_groups(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError> {
        crate::operations::group::get_group(self.runtime(), group_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError> {
        crate::operations::group::resolve_group_identity_scope_name(self.runtime(), group_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageIdentityGroup>, StorageError> {
        crate::operations::group::create_group(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageIdentityGroup>, StorageError> {
        crate::operations::group::update_group(self.runtime(), group_id.id(), update, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        crate::operations::group::delete_group(self.runtime(), group_id.id(), context)
            .await
            .map_err(StorageError::from)
    }

    async fn list_all_group_members(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError> {
        crate::operations::group::list_all_group_members(self.runtime(), group_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_group_members_page(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<StoragePage<StorageGroupMember>, StorageError> {
        crate::operations::group::list_group_members_page(
            self.runtime(),
            group_id.id(),
            query_options,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn add_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StoragePrincipalGroup>, StorageError> {
        crate::operations::group::add_group_member(
            self.runtime(),
            principal_id.id(),
            group_id.id(),
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn remove_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::group::remove_group_member(
            self.runtime(),
            principal_id.id(),
            group_id.id(),
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl PrincipalStorage for PostgresStorage {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        crate::operations::principal::get_principal(self.runtime(), principal_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        crate::operations::principal::get_principal_settings(self.runtime(), principal_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn update_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StoragePrincipalSettings>, StorageError> {
        crate::operations::principal::update_principal_settings(
            self.runtime(),
            principal_id.id(),
            mutation,
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl CollectionAuthorizationQueryStorage for PostgresStorage {
    async fn list_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        crate::operations::authorization::list_principal_collection_permissions(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        crate::operations::authorization::list_all_principal_collection_permissions(
            self.runtime(),
            principal_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_principal_collection_permissions_page(
        &self,
        query: AuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<AuthorizationGroupGrant>, StorageError> {
        crate::operations::authorization::list_principal_collection_permissions_page(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        crate::operations::authorization::list_effective_principal_collection_permissions(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_visible_collections(
        &self,
        query: AuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        crate::operations::authorization::list_visible_collections(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn has_group_collection_permission(
        &self,
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        crate::operations::authorization::has_group_collection_permission(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        crate::operations::authorization::list_effective_group_collection_permissions(
            self.runtime(),
            collection_id.id(),
            group_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        crate::operations::authorization::list_groups_with_collection_permission(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_groups_with_collection_permission_page(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<AuthorizationGroup>, StorageError> {
        crate::operations::authorization::list_groups_with_collection_permission_page(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_collection_group_permissions(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        let (rows, _) =
            crate::operations::authorization::list_local_collection_grants(self.runtime(), query)
                .await
                .map_err(StorageError::from)?
                .into_parts();
        Ok(rows)
    }

    async fn list_collection_group_permissions_page(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<AuthorizationGroupGrant>, StorageError> {
        crate::operations::authorization::list_local_collection_grants(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_collection_group_permission(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<AuthorizationGrant, StorageError> {
        crate::operations::authorization::get_collection_group_permission(
            self.runtime(),
            collection_id.id(),
            group_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }
}
