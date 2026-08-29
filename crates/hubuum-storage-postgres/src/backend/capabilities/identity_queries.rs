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
    ) -> Result<StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        crate::operations::group::create_group(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        crate::operations::group::update_group(self.runtime(), group_id.id(), update, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        crate::operations::group::delete_group(self.runtime(), group_id.id(), context)
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
    ) -> Result<StorageMutationOutcome<StoragePrincipalSettings>, StorageError> {
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
    async fn load_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationGroupGrant>, StorageError> {
        crate::operations::authorization::load_principal_collection_permissions(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        crate::operations::authorization::list_all_principal_collection_permissions(
            self.runtime(),
            principal_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        crate::operations::authorization::list_principal_collection_permissions(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        crate::operations::authorization::list_effective_principal_collection_permissions(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_visible_collections(
        &self,
        query: StorageAuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        crate::operations::authorization::list_visible_collections(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn has_group_collection_permission(
        &self,
        query: StorageAuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        crate::operations::authorization::has_group_collection_permission(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        crate::operations::authorization::list_effective_group_collection_permissions(
            self.runtime(),
            collection_id.id(),
            group_id.id(),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn load_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<StorageAuthorizationGroup>, StorageError> {
        crate::operations::authorization::load_groups_with_collection_permission(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroup>, StorageError> {
        crate::operations::authorization::list_groups_with_collection_permission(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }
}
