use super::super::*;

#[async_trait]
impl GroupStorage for PostgresStorage {
    async fn load_group(&self, group_id: i32) -> Result<StorageIdentityGroup, StorageError> {
        hubuum_storage_postgres::operations::group::load_group(self.runtime(), group_id)
            .await
            .map_err(StorageError::from)
    }

    async fn group_identity_scope_name(&self, group_id: i32) -> Result<String, StorageError> {
        hubuum_storage_postgres::operations::group::group_identity_scope_name(
            self.runtime(),
            group_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        hubuum_storage_postgres::operations::group::create_group(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_group(
        &self,
        group_id: i32,
        update: StorageGroupUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        hubuum_storage_postgres::operations::group::update_group(
            self.runtime(),
            group_id,
            update,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_group(
        &self,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<usize, StorageError> {
        hubuum_storage_postgres::operations::group::delete_group(self.runtime(), group_id, context)
            .await
            .map_err(StorageError::from)
    }

    async fn group_members(&self, group_id: i32) -> Result<Vec<StoragePrincipal>, StorageError> {
        hubuum_storage_postgres::operations::group::group_members(self.runtime(), group_id)
            .await
            .map_err(StorageError::from)
    }

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<Vec<(StoragePrincipalGroup, StoragePrincipal)>, StorageError> {
        hubuum_storage_postgres::operations::group::group_members_page(
            self.runtime(),
            group_id,
            query_options,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<i64, StorageError> {
        hubuum_storage_postgres::operations::group::count_group_members(
            self.runtime(),
            group_id,
            query_options,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn group_member_principal(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipal, StorageError> {
        hubuum_storage_postgres::operations::group::group_member_principal(
            self.runtime(),
            principal_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        hubuum_storage_postgres::operations::group::add_group_member(
            self.runtime(),
            principal_id,
            group_id,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn remove_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::group::remove_group_member(
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
        hubuum_storage_postgres::operations::principal::load_principal(self.runtime(), principal_id)
            .await
            .map_err(StorageError::from)
    }

    async fn load_principal_settings(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        hubuum_storage_postgres::operations::principal::load_principal_settings(
            self.runtime(),
            principal_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn mutate_principal_settings(
        &self,
        principal_id: i32,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        hubuum_storage_postgres::operations::principal::mutate_principal_settings(
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
        hubuum_storage_postgres::operations::authorization::principal_collection_permissions(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn principal_all_collection_permissions(
        &self,
        principal_id: i32,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        hubuum_storage_postgres::operations::authorization::principal_all_collection_permissions(
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
        hubuum_storage_postgres::operations::authorization::principal_collection_permissions_page(
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
        hubuum_storage_postgres::operations::authorization::effective_principal_collection_permissions(
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
        hubuum_storage_postgres::operations::authorization::visible_collections(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn group_has_collection_permission(
        &self,
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        hubuum_storage_postgres::operations::authorization::group_has_collection_permission(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn effective_group_collection_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        hubuum_storage_postgres::operations::authorization::effective_group_collection_permissions(
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
        hubuum_storage_postgres::operations::authorization::groups_with_collection_permission(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn groups_with_collection_permission_page(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<AuthorizationGroupPage, StorageError> {
        hubuum_storage_postgres::operations::authorization::groups_with_collection_permission_page(
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
            hubuum_storage_postgres::operations::authorization::list_local_collection_grants(
                self.runtime(),
                query,
            )
            .await
            .map_err(StorageError::from)?
            .into_parts();
        Ok(rows)
    }

    async fn list_collection_group_permissions_page(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        hubuum_storage_postgres::operations::authorization::list_local_collection_grants(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn collection_group_permission(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<AuthorizationGrant, StorageError> {
        hubuum_storage_postgres::operations::authorization::collection_group_permission(
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
    async fn get_collection(&self, id: i32) -> Result<StorageCollection, StorageError> {
        hubuum_storage_postgres::operations::collection::get_collection(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        hubuum_storage_postgres::operations::collection::create_collection(
            self.runtime(),
            command,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn update_collection(
        &self,
        id: i32,
        changes: StorageCollectionUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        hubuum_storage_postgres::operations::collection::update_collection(
            self.runtime(),
            id,
            changes,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_collection(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::collection::delete_collection(
            self.runtime(),
            id,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn collection_children(&self, id: i32) -> Result<Vec<StorageCollection>, StorageError> {
        hubuum_storage_postgres::operations::collection::collection_children(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }

    async fn collection_ancestors(&self, id: i32) -> Result<Vec<StorageCollection>, StorageError> {
        hubuum_storage_postgres::operations::collection::collection_ancestors(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }

    async fn move_collection(
        &self,
        id: i32,
        new_parent_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError> {
        hubuum_storage_postgres::operations::collection::move_collection(
            self.runtime(),
            id,
            new_parent_id,
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
        hubuum_storage_postgres::operations::class::resolve_class(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError> {
        hubuum_storage_postgres::operations::class::create_class(self.runtime(), command, context)
            .await
            .map_err(StorageError::from)
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError> {
        hubuum_storage_postgres::operations::class::update_class(
            self.runtime(),
            target,
            changes,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::class::delete_class(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn class_names(&self, class_ids: Vec<i32>) -> Result<Vec<(i32, String)>, StorageError> {
        hubuum_storage_postgres::operations::class::class_names(self.runtime(), class_ids)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ClassRelationStore for PostgresStorage {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        hubuum_storage_postgres::operations::relation::prepare_class_relation(
            self.runtime(),
            command,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn resolve_class_relation(
        &self,
        id: i32,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        hubuum_storage_postgres::operations::relation::resolve_class_relation(self.runtime(), id)
            .await
            .map_err(StorageError::from)
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        hubuum_storage_postgres::operations::relation::create_class_relation(
            self.runtime(),
            prepared,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::relation::delete_class_relation(
            self.runtime(),
            target,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn create_class_relation_from_command(
        &self,
        command: StorageClassRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRelation, StorageError> {
        hubuum_storage_postgres::operations::relation::create_class_relation_from_command(
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
        hubuum_storage_postgres::operations::relation::delete_class_relation_by_id(
            self.runtime(),
            id,
            context,
        )
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
        hubuum_storage_postgres::operations::relation::prepare_object_relation(
            self.runtime(),
            selector,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        hubuum_storage_postgres::operations::relation::resolve_object_relation(
            self.runtime(),
            selector,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        hubuum_storage_postgres::operations::relation::create_object_relation(
            self.runtime(),
            prepared,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::relation::delete_object_relation(
            self.runtime(),
            target,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn create_object_relation_from_command(
        &self,
        command: StorageObjectRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageObjectRelation, StorageError> {
        hubuum_storage_postgres::operations::relation::create_object_relation_from_command(
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
        hubuum_storage_postgres::operations::relation::delete_object_relation_by_id(
            self.runtime(),
            id,
            context,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectStore for PostgresStorage {
    async fn get_object(&self, object_id: i32) -> Result<StorageResolvedObject, StorageError> {
        hubuum_storage_postgres::operations::object::get_object(self.runtime(), object_id)
            .await
            .map_err(StorageError::from)
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        hubuum_storage_postgres::operations::object::resolve_object(self.runtime(), selector)
            .await
            .map_err(StorageError::from)
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError> {
        hubuum_storage_postgres::operations::object::create_object(
            self.runtime(),
            class,
            command,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError> {
        hubuum_storage_postgres::operations::object::update_object(
            self.runtime(),
            target,
            changes,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<StorageObject, StorageError> {
        hubuum_storage_postgres::operations::object::patch_object_data(
            self.runtime(),
            target,
            patch,
            context,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::object::delete_object(self.runtime(), target, context)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::object::validate_object(self.runtime(), object)
            .await
            .map_err(StorageError::from)
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::object::validate_object_create_command(
            self.runtime(),
            command,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn validate_object_update(
        &self,
        object_id: i32,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::object::validate_object_update_command(
            self.runtime(),
            object_id,
            changes,
        )
        .await
        .map_err(StorageError::from)
    }
}
