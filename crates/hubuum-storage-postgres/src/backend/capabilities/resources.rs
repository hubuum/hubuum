use super::super::*;

#[async_trait]
impl GroupStorage for PostgresStorage {
    async fn load_group(&self, group_id: i32) -> Result<StorageIdentityGroup, StorageError> {
        crate::operations::group::load_group(self.runtime(), group_id)
            .await
            .map_err(StorageError::from)
    }

    async fn group_identity_scope_name(&self, group_id: i32) -> Result<String, StorageError> {
        crate::operations::group::group_identity_scope_name(self.runtime(), group_id)
            .await
            .map_err(StorageError::from)
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        crate::operations::group::create_group(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_group(
        &self,
        group_id: i32,
        update: StorageGroupUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        crate::operations::group::update_group(self.runtime(), group_id, update, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_group(
        &self,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<usize, StorageError> {
        crate::operations::group::delete_group(self.runtime(), group_id, context)
            .await
            .map_err(StorageError::from)
    }

    async fn group_members(&self, group_id: i32) -> Result<Vec<StoragePrincipal>, StorageError> {
        crate::operations::group::group_members(self.runtime(), group_id)
            .await
            .map_err(StorageError::from)
    }

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<Vec<(StoragePrincipalGroup, StoragePrincipal)>, StorageError> {
        crate::operations::group::group_members_page(self.runtime(), group_id, query_options)
            .await
            .map_err(StorageError::from)
    }

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<i64, StorageError> {
        crate::operations::group::count_group_members(self.runtime(), group_id, query_options)
            .await
            .map_err(StorageError::from)
    }

    async fn group_member_principal(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipal, StorageError> {
        crate::operations::group::group_member_principal(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        crate::operations::group::add_group_member(self.runtime(), principal_id, group_id, context)
            .await
            .map_err(StorageError::from)
    }

    async fn remove_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        crate::operations::group::remove_group_member(
            self.runtime(),
            principal_id,
            group_id,
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl PrincipalStorage for PostgresStorage {
    async fn load_principal(&self, principal_id: i32) -> Result<StoragePrincipal, StorageError> {
        crate::operations::principal::load_principal(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }

    async fn load_principal_settings(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        crate::operations::principal::load_principal_settings(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }

    async fn mutate_principal_settings(
        &self,
        principal_id: i32,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        crate::operations::principal::mutate_principal_settings(
            self.runtime(),
            principal_id,
            mutation,
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl CollectionAuthorizationStorage for PostgresStorage {
    async fn principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        crate::operations::authorization::principal_collection_permissions(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn principal_all_collection_permissions(
        &self,
        principal_id: i32,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        crate::operations::authorization::principal_all_collection_permissions(
            self.runtime(),
            principal_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn principal_collection_permissions_page(
        &self,
        query: AuthorizationPrincipalCollectionPageQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        crate::operations::authorization::principal_collection_permissions_page(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn effective_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        crate::operations::authorization::effective_principal_collection_permissions(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn visible_collections(
        &self,
        query: AuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        crate::operations::authorization::visible_collections(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn group_has_collection_permission(
        &self,
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        crate::operations::authorization::group_has_collection_permission(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn effective_group_collection_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        crate::operations::authorization::effective_group_collection_permissions(
            self.runtime(),
            collection_id,
            group_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        crate::operations::authorization::groups_with_collection_permission(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn groups_with_collection_permission_page(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<AuthorizationGroupPage, StorageError> {
        crate::operations::authorization::groups_with_collection_permission_page(
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
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        crate::operations::authorization::list_local_collection_grants(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn collection_group_permission(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<AuthorizationGrant, StorageError> {
        crate::operations::authorization::collection_group_permission(
            self.runtime(),
            collection_id,
            group_id,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl CollectionStore for PostgresStorage {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        crate::operations::collection::get_collection(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        crate::operations::collection::create_collection(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        crate::operations::collection::update_collection(self.runtime(), id.id(), changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        crate::operations::collection::delete_collection(self.runtime(), id.id(), context)
            .await
            .map_err(StorageError::from)
    }

    async fn collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        crate::operations::collection::collection_children(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        crate::operations::collection::collection_ancestors(self.runtime(), id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
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
impl ClassStore for PostgresStorage {
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
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError> {
        crate::operations::class::create_class(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError> {
        crate::operations::class::update_class(self.runtime(), target, changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        crate::operations::class::delete_class(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        let class_ids = class_ids.into_iter().map(ClassId::id).collect();
        crate::operations::class::class_names(self.runtime(), class_ids)
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
impl ClassRelationStore for PostgresStorage {
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
        id: i32,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        crate::operations::relation::resolve_class_relation(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        crate::operations::relation::create_class_relation(self.runtime(), prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        crate::operations::relation::delete_class_relation(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn create_class_relation_from_command(
        &self,
        command: StorageClassRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRelation, StorageError> {
        crate::operations::relation::create_class_relation_from_command(
            self.runtime(),
            command,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_class_relation_by_id(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        crate::operations::relation::delete_class_relation_by_id(self.runtime(), id, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectRelationStore for PostgresStorage {
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
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        crate::operations::relation::create_object_relation(self.runtime(), prepared, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        crate::operations::relation::delete_object_relation(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object_relation_from_command(
        &self,
        command: StorageObjectRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageObjectRelation, StorageError> {
        crate::operations::relation::create_object_relation_from_command(
            self.runtime(),
            command,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_object_relation_by_id(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        crate::operations::relation::delete_object_relation_by_id(self.runtime(), id, context)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectStore for PostgresStorage {
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
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError> {
        crate::operations::object::create_object(self.runtime(), class, command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError> {
        crate::operations::object::update_object(self.runtime(), target, changes, context)
            .await
            .map_err(StorageError::from)
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<StorageObject, StorageError> {
        crate::operations::object::patch_object_data(self.runtime(), target, patch, context)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
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
