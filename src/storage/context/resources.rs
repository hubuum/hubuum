use super::*;

#[async_trait]
impl GroupStorage for StorageHandle {
    async fn load_group(&self, group_id: i32) -> Result<StorageIdentityGroup, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "load", async {
            dispatch_backend!(self, |backend| backend.load_group(group_id).await)
        })
        .await
    }

    async fn group_identity_scope_name(&self, group_id: i32) -> Result<String, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "identity_scope", async {
            dispatch_backend!(self, |backend| {
                backend.group_identity_scope_name(group_id).await
            })
        })
        .await
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "create", async {
            dispatch_backend!(self, |backend| {
                backend.create_group(command, context).await
            })
        })
        .await
    }

    async fn update_group(
        &self,
        group_id: i32,
        update: StorageGroupUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "update", async {
            dispatch_backend!(self, |backend| {
                backend.update_group(group_id, update, context).await
            })
        })
        .await
    }

    async fn delete_group(
        &self,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<usize, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "delete", async {
            dispatch_backend!(self, |backend| {
                backend.delete_group(group_id, context).await
            })
        })
        .await
    }

    async fn group_members(&self, group_id: i32) -> Result<Vec<StoragePrincipal>, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "members", async {
            dispatch_backend!(self, |backend| backend.group_members(group_id).await)
        })
        .await
    }

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<Vec<(StoragePrincipalGroup, StoragePrincipal)>, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "members_page", async {
            dispatch_backend!(self, |backend| {
                backend.group_members_page(group_id, query_options).await
            })
        })
        .await
    }

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<i64, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "members_count", async {
            dispatch_backend!(self, |backend| {
                backend.count_group_members(group_id, query_options).await
            })
        })
        .await
    }

    async fn group_member_principal(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipal, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "member_principal", async {
            dispatch_backend!(self, |backend| {
                backend.group_member_principal(principal_id).await
            })
        })
        .await
    }

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "member_add", async {
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
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "groups", "member_remove", async {
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
    async fn load_principal(&self, principal_id: i32) -> Result<StoragePrincipal, StorageError> {
        observe_storage_call(self.backend_name(), "principals", "load", async {
            dispatch_backend!(self, |backend| {
                backend.load_principal(principal_id).await
            })
        })
        .await
    }

    async fn load_principal_settings(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        observe_storage_call(self.backend_name(), "principals", "settings_load", async {
            dispatch_backend!(self, |backend| {
                backend.load_principal_settings(principal_id).await
            })
        })
        .await
    }

    async fn mutate_principal_settings(
        &self,
        principal_id: i32,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        let operation = match &mutation {
            StoragePrincipalSettingsMutation::Replace(_) => "settings_replace",
            StoragePrincipalSettingsMutation::MergePatch(_) => "settings_merge",
            StoragePrincipalSettingsMutation::JsonPatch(_) => "settings_json_patch",
            StoragePrincipalSettingsMutation::Reset => "settings_reset",
        };
        observe_storage_call(self.backend_name(), "principals", operation, async {
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
    async fn principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "principal",
            async {
                dispatch_backend!(self, |backend| {
                    backend.principal_collection_permissions(query).await
                })
            },
        )
        .await
    }

    async fn principal_all_collection_permissions(
        &self,
        principal_id: i32,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "principal_all",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .principal_all_collection_permissions(principal_id)
                        .await
                })
            },
        )
        .await
    }

    async fn principal_collection_permissions_page(
        &self,
        query: AuthorizationPrincipalCollectionPageQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "principal_page",
            async {
                dispatch_backend!(self, |backend| {
                    backend.principal_collection_permissions_page(query).await
                })
            },
        )
        .await
    }

    async fn effective_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "effective_principal",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .effective_principal_collection_permissions(query)
                        .await
                })
            },
        )
        .await
    }

    async fn visible_collections(
        &self,
        query: AuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "visible",
            async {
                dispatch_backend!(self, |backend| { backend.visible_collections(query).await })
            },
        )
        .await
    }

    async fn group_has_collection_permission(
        &self,
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "group_has",
            async {
                dispatch_backend!(self, |backend| {
                    backend.group_has_collection_permission(query).await
                })
            },
        )
        .await
    }

    async fn effective_group_collection_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "effective_group",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .effective_group_collection_permissions(collection_id, group_id)
                        .await
                })
            },
        )
        .await
    }

    async fn groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "groups",
            async {
                dispatch_backend!(self, |backend| {
                    backend.groups_with_collection_permission(query).await
                })
            },
        )
        .await
    }

    async fn groups_with_collection_permission_page(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<AuthorizationGroupPage, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "groups_page",
            async {
                dispatch_backend!(self, |backend| {
                    backend.groups_with_collection_permission_page(query).await
                })
            },
        )
        .await
    }

    async fn list_collection_group_permissions(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        observe_storage_call(
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
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        observe_storage_call(
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

    async fn collection_group_permission(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<AuthorizationGrant, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "collection_permissions",
            "group_grant",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .collection_group_permission(collection_id, group_id)
                        .await
                })
            },
        )
        .await
    }
}
