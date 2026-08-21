use super::*;

#[async_trait]
impl GroupStorage for StorageHandle {
    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "get", async {
            dispatch_backend!(self, |backend| backend.get_group(group_id).await)
        })
        .await
    }

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "identity_scope", async {
            dispatch_backend!(self, |backend| {
                backend.resolve_group_identity_scope_name(group_id).await
            })
        })
        .await
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageIdentityGroup>, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "create", async {
            dispatch_backend!(self, |backend| {
                backend.create_group(command, context).await
            })
        })
        .await
    }

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageIdentityGroup>, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "update", async {
            dispatch_backend!(self, |backend| {
                backend.update_group(group_id, update, context).await
            })
        })
        .await
    }

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<usize>, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "delete", async {
            dispatch_backend!(self, |backend| {
                backend.delete_group(group_id, context).await
            })
        })
        .await
    }

    async fn list_group_members(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "members", async {
            dispatch_backend!(self, |backend| backend.list_group_members(group_id).await)
        })
        .await
    }

    async fn list_group_members_page(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<Vec<(StoragePrincipalGroup, StoragePrincipal)>, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "members_page", async {
            dispatch_backend!(self, |backend| {
                backend
                    .list_group_members_page(group_id, query_options)
                    .await
            })
        })
        .await
    }

    async fn count_group_members(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<i64, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "members_count", async {
            dispatch_backend!(self, |backend| {
                backend.count_group_members(group_id, query_options).await
            })
        })
        .await
    }

    async fn get_group_member_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "member_principal", async {
            dispatch_backend!(self, |backend| {
                backend.get_group_member_principal(principal_id).await
            })
        })
        .await
    }

    async fn add_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StoragePrincipalGroup>, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "member_add", async {
            dispatch_backend!(self, |backend| {
                backend
                    .add_group_member(principal_id, group_id, context)
                    .await
            })
        })
        .await
    }

    async fn remove_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(self.backend_name(), "groups", "member_remove", async {
            dispatch_backend!(self, |backend| {
                backend
                    .remove_group_member(principal_id, group_id, context)
                    .await
            })
        })
        .await
    }
}

#[async_trait]
impl PrincipalStorage for StorageHandle {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        self.observe_storage_call(self.backend_name(), "principals", "get", async {
            dispatch_backend!(self, |backend| {
                backend.get_principal(principal_id).await
            })
        })
        .await
    }

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        self.observe_storage_call(self.backend_name(), "principals", "get_settings", async {
            dispatch_backend!(self, |backend| {
                backend.get_principal_settings(principal_id).await
            })
        })
        .await
    }

    async fn mutate_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StoragePrincipalSettings>, StorageError> {
        let operation = match &mutation {
            StoragePrincipalSettingsMutation::Replace(_) => "settings_replace",
            StoragePrincipalSettingsMutation::MergePatch(_) => "settings_merge",
            StoragePrincipalSettingsMutation::JsonPatch(_) => "settings_json_patch",
            StoragePrincipalSettingsMutation::Reset => "settings_reset",
        };
        self.observe_storage_call(self.backend_name(), "principals", operation, async {
            dispatch_backend!(self, |backend| {
                backend
                    .mutate_principal_settings(principal_id, mutation, context)
                    .await
            })
        })
        .await
    }
}

#[async_trait]
impl CollectionAuthorizationStorage for StorageHandle {
    async fn list_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "principal",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_principal_collection_permissions(query).await
                })
            },
        )
        .await
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "principal_all",
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

    async fn list_principal_collection_permissions_page(
        &self,
        query: AuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StorageCountedPage<AuthorizationGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "principal_page",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_principal_collection_permissions_page(query)
                        .await
                })
            },
        )
        .await
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "effective_principal",
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
        query: AuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "visible",
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
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "group_has",
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
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "effective_group",
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

    async fn list_groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "groups",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_groups_with_collection_permission(query).await
                })
            },
        )
        .await
    }

    async fn list_groups_with_collection_permission_page(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<StorageCountedPage<AuthorizationGroup>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "groups_page",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_groups_with_collection_permission_page(query)
                        .await
                })
            },
        )
        .await
    }

    async fn list_collection_group_permissions(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "grants",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_collection_group_permissions(query).await
                })
            },
        )
        .await
    }

    async fn list_collection_group_permissions_page(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<StorageCountedPage<AuthorizationGroupGrant>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "grants_page",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_collection_group_permissions_page(query).await
                })
            },
        )
        .await
    }

    async fn get_collection_group_permission(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<AuthorizationGrant, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "group_grant",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .get_collection_group_permission(collection_id, group_id)
                        .await
                })
            },
        )
        .await
    }
}
