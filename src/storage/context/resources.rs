use super::*;

#[async_trait]
impl GroupStorage for StorageHandle {
    async fn load_group(&self, group_id: i32) -> Result<Group, StorageError> {
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
        command: &NewGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError> {
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
        update: &UpdateGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError> {
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

    async fn group_members(&self, group_id: i32) -> Result<Vec<Principal>, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "members", async {
            dispatch_backend!(self, |backend| backend.group_members(group_id).await)
        })
        .await
    }

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: &QueryOptions,
    ) -> Result<Vec<(PrincipalGroup, Principal)>, StorageError> {
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
        query_options: &QueryOptions,
    ) -> Result<i64, StorageError> {
        observe_storage_call(self.backend_name(), "groups", "members_count", async {
            dispatch_backend!(self, |backend| {
                backend.count_group_members(group_id, query_options).await
            })
        })
        .await
    }

    async fn group_member_principal(&self, principal_id: i32) -> Result<Principal, StorageError> {
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
    ) -> Result<PrincipalGroup, StorageError> {
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
    async fn load_principal(&self, principal_id: i32) -> Result<Principal, StorageError> {
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
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        observe_storage_call(self.backend_name(), "principals", "settings_load", async {
            dispatch_backend!(self, |backend| {
                backend.load_principal_settings(principal_id).await
            })
        })
        .await
    }

    async fn replace_principal_settings(
        &self,
        principal_id: i32,
        settings: PrincipalSettings,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "principals",
            "settings_replace",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .replace_principal_settings(principal_id, settings, context)
                        .await
                })
            },
        )
        .await
    }

    async fn merge_principal_settings(
        &self,
        principal_id: i32,
        patch: PrincipalSettings,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        observe_storage_call(self.backend_name(), "principals", "settings_merge", async {
            dispatch_backend!(self, |backend| {
                backend
                    .merge_principal_settings(principal_id, patch, context)
                    .await
            })
        })
        .await
    }

    async fn apply_principal_settings_patch(
        &self,
        principal_id: i32,
        patch: PrincipalSettingsPatch,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "principals",
            "settings_json_patch",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .apply_principal_settings_patch(principal_id, patch, context)
                        .await
                })
            },
        )
        .await
    }

    async fn reset_principal_settings(
        &self,
        principal_id: i32,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        observe_storage_call(self.backend_name(), "principals", "settings_reset", async {
            dispatch_backend!(self, |backend| {
                backend
                    .reset_principal_settings(principal_id, context)
                    .await
            })
        })
        .await
    }
}

#[async_trait]
impl CollectionRecordStorage for StorageHandle {
    async fn create_collection_record(
        &self,
        command: &NewCollectionWithAssignee,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        observe_storage_call(self.backend_name(), "collection_records", "create", async {
            dispatch_backend!(self, |backend| {
                backend.create_collection_record(command, context).await
            })
        })
        .await
    }

    async fn update_collection_record(
        &self,
        update: &UpdateCollection,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        observe_storage_call(self.backend_name(), "collection_records", "update", async {
            dispatch_backend!(self, |backend| {
                backend
                    .update_collection_record(update, collection_id, context)
                    .await
            })
        })
        .await
    }

    async fn delete_collection_record(
        &self,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "collection_records", "delete", async {
            dispatch_backend!(self, |backend| {
                backend
                    .delete_collection_record(collection_id, context)
                    .await
            })
        })
        .await
    }

    async fn move_collection_record(
        &self,
        collection_id: i32,
        new_parent_collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        observe_storage_call(self.backend_name(), "collection_records", "move", async {
            dispatch_backend!(self, |backend| {
                backend
                    .move_collection_record(collection_id, new_parent_collection_id, context)
                    .await
            })
        })
        .await
    }
}

#[async_trait]
impl CollectionPermissionStorage for StorageHandle {
    async fn principal_collection_permissions(
        &self,
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<GroupPermission>, StorageError> {
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
    ) -> Result<Vec<(Collection, Group, Permission)>, StorageError> {
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
        query: CollectionPrincipalPageQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError> {
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
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError> {
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
        query: CollectionVisibilityQuery,
    ) -> Result<Vec<Collection>, StorageError> {
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
        query: CollectionGroupPermissionQuery,
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
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError> {
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
        query: CollectionGroupsQuery,
    ) -> Result<Vec<Group>, StorageError> {
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
        query: CollectionGroupsPageQuery,
    ) -> Result<(Vec<Group>, i64), StorageError> {
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
        query: CollectionGrantListQuery,
    ) -> Result<Vec<GroupPermission>, StorageError> {
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
        query: CollectionGrantListQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError> {
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
    ) -> Result<Permission, StorageError> {
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

#[async_trait]
impl ClassRecordStorage for StorageHandle {
    async fn create_class_record(
        &self,
        class: &NewHubuumClass,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError> {
        observe_storage_call(self.backend_name(), "class_records", "create", async {
            dispatch_backend!(self, |backend| {
                backend.create_class_record(class, context).await
            })
        })
        .await
    }

    async fn update_class_record(
        &self,
        update: &UpdateHubuumClass,
        class_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError> {
        observe_storage_call(self.backend_name(), "class_records", "update", async {
            dispatch_backend!(self, |backend| {
                backend.update_class_record(update, class_id, context).await
            })
        })
        .await
    }

    async fn delete_class_record(
        &self,
        class: &HubuumClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "class_records", "delete", async {
            dispatch_backend!(self, |backend| {
                backend.delete_class_record(class, context).await
            })
        })
        .await
    }

    async fn load_class_record(&self, class_id: i32) -> Result<HubuumClass, StorageError> {
        observe_storage_call(self.backend_name(), "class_records", "load", async {
            dispatch_backend!(self, |backend| {
                backend.load_class_record(class_id).await
            })
        })
        .await
    }

    async fn class_collection(&self, class_id: i32) -> Result<Collection, StorageError> {
        observe_storage_call(self.backend_name(), "class_records", "collection", async {
            dispatch_backend!(self, |backend| { backend.class_collection(class_id).await })
        })
        .await
    }

    async fn class_names(
        &self,
        class_ids: &ClassIdSet,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        observe_storage_call(self.backend_name(), "class_records", "names", async {
            dispatch_backend!(self, |backend| backend.class_names(class_ids).await)
        })
        .await
    }
}

#[async_trait]
impl ObjectRecordStorage for StorageHandle {
    async fn validate_object(&self, object: &HubuumObject) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "validate", async {
            dispatch_backend!(self, |backend| backend.validate_object(object).await)
        })
        .await
    }

    async fn validate_new_object(&self, object: &NewHubuumObject) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "object_records",
            "validate_new",
            async {
                dispatch_backend!(self, |backend| {
                    backend.validate_new_object(object).await
                })
            },
        )
        .await
    }

    async fn validate_object_update(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "object_records",
            "validate_update",
            async {
                dispatch_backend!(self, |backend| {
                    backend.validate_object_update(update, object_id).await
                })
            },
        )
        .await
    }

    async fn save_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "save", async {
            dispatch_backend!(self, |backend| {
                backend.save_object_record(object, context).await
            })
        })
        .await
    }

    async fn create_object_record(
        &self,
        object: &NewHubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "create", async {
            dispatch_backend!(self, |backend| {
                backend.create_object_record(object, context).await
            })
        })
        .await
    }

    async fn update_object_record(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "update", async {
            dispatch_backend!(self, |backend| {
                backend
                    .update_object_record(update, object_id, context)
                    .await
            })
        })
        .await
    }

    async fn delete_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "delete", async {
            dispatch_backend!(self, |backend| {
                backend.delete_object_record(object, context).await
            })
        })
        .await
    }

    async fn load_object_record(&self, object_id: i32) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "load", async {
            dispatch_backend!(self, |backend| {
                backend.load_object_record(object_id).await
            })
        })
        .await
    }

    async fn object_collection(&self, object_id: i32) -> Result<Collection, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "collection", async {
            dispatch_backend!(self, |backend| {
                backend.object_collection(object_id).await
            })
        })
        .await
    }

    async fn object_class(&self, object_id: i32) -> Result<HubuumClass, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "class", async {
            dispatch_backend!(self, |backend| backend.object_class(object_id).await)
        })
        .await
    }
}
