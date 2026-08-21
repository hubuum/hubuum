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

    async fn list_group_members(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError> {
        crate::operations::group::list_group_members(self.runtime(), group_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_group_members_page(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<Vec<(StoragePrincipalGroup, StoragePrincipal)>, StorageError> {
        crate::operations::group::list_group_members_page(
            self.runtime(),
            group_id.id(),
            query_options,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn count_group_members(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<i64, StorageError> {
        crate::operations::group::count_group_members(self.runtime(), group_id.id(), query_options)
            .await
            .map_err(StorageError::from)
    }

    async fn get_group_member_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        crate::operations::group::get_group_member_principal(self.runtime(), principal_id.id())
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

#[async_trait]
impl CollectionStorage for PostgresStorage {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        crate::operations::collection::get_collection(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        crate::operations::collection::create_collection(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        crate::operations::collection::update_collection(self.runtime(), id.id(), changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::collection::delete_collection(self.runtime(), id.id(), context)
            .await
            .map_err(StorageError::from)
    }

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        crate::operations::collection::list_collection_children(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        crate::operations::collection::list_collection_ancestors(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        crate::operations::collection::move_collection(
            self.runtime(),
            id.id(),
            new_parent_id.id(),
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl ClassStorage for PostgresStorage {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        crate::operations::class::resolve_class(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        crate::operations::class::create_class(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError> {
        crate::operations::class::update_class(self.runtime(), target, changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::class::delete_class(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        let class_ids = class_ids.into_iter().map(ClassId::id).collect();
        crate::operations::class::resolve_class_names(self.runtime(), class_ids)
            .await
            .map_err(StorageError::from)
            .and_then(|rows| {
                rows.into_iter()
                    .map(|(id, name)| {
                        ClassId::new(id)
                            .map(|id| (id, name))
                            .map_err(crate::PostgresStorageError::from)
                            .map_err(StorageError::from)
                    })
                    .collect()
            })
    }
}

#[async_trait]
impl ClassRelationStorage for PostgresStorage {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        crate::operations::relation::prepare_class_relation(self.runtime(), command)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        crate::operations::relation::resolve_class_relation(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedClassRelation>, StorageError> {
        crate::operations::relation::create_class_relation(self.runtime(), prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::relation::delete_class_relation(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectRelationStorage for PostgresStorage {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        crate::operations::relation::prepare_object_relation(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        crate::operations::relation::resolve_object_relation(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        crate::operations::relation::create_object_relation(self.runtime(), prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::relation::delete_object_relation(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectStorage for PostgresStorage {
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        crate::operations::object::get_object(self.runtime(), object_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        crate::operations::object::resolve_object(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        crate::operations::object::create_object(self.runtime(), class, command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        crate::operations::object::update_object(self.runtime(), target, changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError> {
        crate::operations::object::patch_object_data(self.runtime(), target, patch, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::object::delete_object(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        crate::operations::object::validate_object(self.runtime(), object)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        crate::operations::object::validate_object_create_command(self.runtime(), command)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        crate::operations::object::validate_object_update_command(
            self.runtime(),
            object_id.id(),
            changes,
        )
        .await
        .map_err(StorageError::from)
    }
}
