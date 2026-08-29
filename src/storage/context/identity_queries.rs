use super::*;

#[async_trait]
impl GroupStorage for StorageHandle {
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Group,
            "list_groups",
            async { dispatch_backend!(self, |backend| backend.list_groups(query).await) },
        )
        .await
    }

    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Group,
            "get_group",
            async { dispatch_backend!(self, |backend| backend.get_group(group_id).await) },
        )
        .await
    }

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Group,
            "resolve_group_identity_scope_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.resolve_group_identity_scope_name(group_id).await
                })
            },
        )
        .await
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Group,
            "create_group",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_group(command, context).await
                })
            },
        )
        .await
    }

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Group,
            "update_group",
            async {
                dispatch_backend!(self, |backend| {
                    backend.update_group(group_id, update, context).await
                })
            },
        )
        .await
    }

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Group,
            "delete_group",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_group(group_id, context).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl PrincipalStorage for StorageHandle {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Principal,
            "get_principal",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_principal(principal_id).await
                })
            },
        )
        .await
    }

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Principal,
            "get_principal_settings",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_principal_settings(principal_id).await
                })
            },
        )
        .await
    }

    async fn update_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StoragePrincipalSettings>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Principal,
            "update_principal_settings",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .update_principal_settings(principal_id, mutation, context)
                        .await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl CollectionAuthorizationQueryStorage for StorageHandle {
    async fn load_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "load_principal_collection_permissions",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_principal_collection_permissions(query).await
                })
            },
        )
        .await
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "list_all_principal_collection_permissions",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_all_principal_collection_permissions(principal_id)
                        .await
                })
            },
        )
        .await
    }

    async fn list_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "list_principal_collection_permissions",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_principal_collection_permissions(query).await
                })
            },
        )
        .await
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "list_effective_principal_collection_permissions",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_effective_principal_collection_permissions(query)
                        .await
                })
            },
        )
        .await
    }

    async fn list_visible_collections(
        &self,
        query: StorageAuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "list_visible_collections",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_visible_collections(query).await
                })
            },
        )
        .await
    }

    async fn has_group_collection_permission(
        &self,
        query: StorageAuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "has_group_collection_permission",
            async {
                dispatch_backend!(self, |backend| {
                    backend.has_group_collection_permission(query).await
                })
            },
        )
        .await
    }

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "list_effective_group_collection_permissions",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_effective_group_collection_permissions(collection_id, group_id)
                        .await
                })
            },
        )
        .await
    }

    async fn load_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<StorageAuthorizationGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "load_groups_with_collection_permission",
            async {
                dispatch_backend!(self, |backend| {
                    backend.load_groups_with_collection_permission(query).await
                })
            },
        )
        .await
    }

    async fn list_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::CollectionAuthorizationQuery,
            "list_groups_with_collection_permission",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_groups_with_collection_permission(query).await
                })
            },
        )
        .await
    }
}
