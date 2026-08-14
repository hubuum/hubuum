use super::super::*;
use crate::services::storage_boundary::{
    group_create_from_storage, group_to_storage, group_update_from_storage,
    principal_group_to_storage, principal_settings_mutation_from_storage,
    principal_settings_to_storage, principal_to_storage,
};
use crate::storage::postgres::operations::authorization::{
    collection_to_storage as authorization_collection_to_storage,
    grant_to_storage as authorization_grant_to_storage,
    group_grant_to_storage as authorization_group_grant_to_storage,
    group_to_storage as authorization_group_to_storage, permission_from_storage,
};
fn effective_grant_to_storage(row: EffectiveGroupPermission) -> AuthorizationEffectiveGroupGrant {
    AuthorizationEffectiveGroupGrant::new(
        authorization_collection_to_storage(row.target_collection),
        authorization_collection_to_storage(row.source_collection),
        row.depth,
        row.inherited,
        authorization_group_to_storage(row.group),
        authorization_grant_to_storage(row.permission),
    )
}

#[async_trait]
impl GroupStorage for PostgresStorage {
    async fn load_group(&self, group_id: i32) -> Result<StorageIdentityGroup, StorageError> {
        GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .load_group_record(&self.pool)
            .await
            .map(group_to_storage)
            .map_err(map_postgres_error)
    }

    async fn group_identity_scope_name(&self, group_id: i32) -> Result<String, StorageError> {
        GroupID::new(group_id).map_err(map_postgres_error)?;
        operations::group::group_identity_scope_name(&self.pool, group_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        group_create_from_storage(command)
            .save_group_record(&self.pool, context)
            .await
            .map(group_to_storage)
            .map_err(map_postgres_error)
    }

    async fn update_group(
        &self,
        group_id: i32,
        update: StorageGroupUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageIdentityGroup, StorageError> {
        let group_id = GroupID::new(group_id).map_err(map_postgres_error)?;
        group_update_from_storage(update)
            .update_group_record(group_id.id(), &self.pool, context)
            .await
            .map(group_to_storage)
            .map_err(map_postgres_error)
    }

    async fn delete_group(
        &self,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<usize, StorageError> {
        GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .delete_group_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_members(&self, group_id: i32) -> Result<Vec<StoragePrincipal>, StorageError> {
        let group = GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .load_group_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        group
            .load_group_members(&self.pool)
            .await
            .map(|members| members.into_iter().map(principal_to_storage).collect())
            .map_err(map_postgres_error)
    }

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<Vec<(StoragePrincipalGroup, StoragePrincipal)>, StorageError> {
        let group = GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .load_group_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        group
            .load_group_members_paginated(&self.pool, &query_options)
            .await
            .map(|members| {
                members
                    .into_iter()
                    .map(|(membership, principal)| {
                        (
                            principal_group_to_storage(membership),
                            principal_to_storage(principal),
                        )
                    })
                    .collect()
            })
            .map_err(map_postgres_error)
    }

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: QueryOptions,
    ) -> Result<i64, StorageError> {
        let group = GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .load_group_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        group
            .count_group_members_paginated(&self.pool, &query_options)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_member_principal(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipal, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::group::group_member_principal(&self.pool, principal_id)
            .await
            .map(principal_to_storage)
            .map_err(map_postgres_error)
    }

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        GroupID::new(group_id).map_err(map_postgres_error)?;
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::group::save_manual_membership(&self.pool, principal_id, group_id, context)
            .await
            .map(principal_group_to_storage)
            .map_err(map_postgres_error)
    }

    async fn remove_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        let group = GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .load_group_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        group
            .remove_group_member_from_backend(principal_id, &self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl PrincipalStorage for PostgresStorage {
    async fn load_principal(&self, principal_id: i32) -> Result<StoragePrincipal, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::load_principal_by_id(&self.pool, principal_id)
            .await
            .map(principal_to_storage)
            .map_err(map_postgres_error)
    }

    async fn load_principal_settings(
        &self,
        principal_id: i32,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::load_principal_settings(&self.pool, principal_id)
            .await
            .map(principal_settings_to_storage)
            .map_err(map_postgres_error)
    }

    async fn mutate_principal_settings(
        &self,
        principal_id: i32,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        let result = match mutation {
            StoragePrincipalSettingsMutation::Replace(value) => {
                operations::principal::mutate_principal_settings(
                    &self.pool,
                    principal_id,
                    operations::principal::PrincipalSettingsMutation::Replace,
                    PrincipalSettings::new(value).map_err(map_postgres_error)?,
                    context,
                )
                .await
            }
            StoragePrincipalSettingsMutation::Reset => {
                operations::principal::mutate_principal_settings(
                    &self.pool,
                    principal_id,
                    operations::principal::PrincipalSettingsMutation::Reset,
                    PrincipalSettings::default(),
                    context,
                )
                .await
            }
            patch => {
                let patch = principal_settings_mutation_from_storage(patch)
                    .map_err(map_postgres_error)?
                    .expect("merge and JSON Patch mutations contain a patch");
                operations::principal::apply_principal_settings_patch(
                    &self.pool,
                    principal_id,
                    patch,
                    context,
                )
                .await
            }
        };
        result
            .map(principal_settings_to_storage)
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CollectionAuthorizationStorage for PostgresStorage {
    async fn principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        principal_on_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
        )
        .await
        .map(|rows| {
            rows.into_iter()
                .map(authorization_group_grant_to_storage)
                .collect()
        })
        .map_err(map_postgres_error)
    }

    async fn principal_all_collection_permissions(
        &self,
        principal_id: i32,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        principal_all_permissions_from_backend(
            &self.pool,
            PrincipalID::new(principal_id).map_err(map_postgres_error)?,
        )
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(collection, group, grant)| {
                    AuthorizationPolicySnapshotRow::new(
                        authorization_grant_to_storage(grant),
                        authorization_group_to_storage(group),
                        authorization_collection_to_storage(collection),
                    )
                })
                .collect()
        })
        .map_err(map_postgres_error)
    }

    async fn principal_collection_permissions_page(
        &self,
        query: AuthorizationPrincipalCollectionPageQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        let (rows, total) = principal_on_paginated_with_total_count_from_backend(
            &self.pool,
            PrincipalID::new(query.principal().principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.principal().collection_id())
                .map_err(map_postgres_error)?
                .id(),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)?;
        Ok(AuthorizationGroupGrantPage::new(
            rows.into_iter()
                .map(authorization_group_grant_to_storage)
                .collect(),
            total,
        ))
    }

    async fn effective_principal_collection_permissions(
        &self,
        query: AuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        effective_principal_on_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
        )
        .await
        .map(|rows| rows.into_iter().map(effective_grant_to_storage).collect())
        .map_err(map_postgres_error)
    }

    async fn visible_collections(
        &self,
        query: AuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        let scope = query
            .scope()
            .cloned()
            .map(operations::identity_operations::token_scope_from_storage)
            .transpose()
            .map_err(map_postgres_error)?;
        user_can_on_any_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            permission_from_storage(query.permission()),
            scope.as_ref(),
        )
        .await
        .map(|rows| {
            rows.into_iter()
                .map(authorization_collection_to_storage)
                .collect()
        })
        .map_err(map_postgres_error)
    }

    async fn group_has_collection_permission(
        &self,
        query: AuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        group_can_on_from_backend(
            &self.pool,
            query.group_id(),
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
            permission_from_storage(query.permission()),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn effective_group_collection_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Vec<AuthorizationEffectiveGroupGrant>, StorageError> {
        effective_group_on_from_backend(&self.pool, collection_id, group_id)
            .await
            .map(|rows| rows.into_iter().map(effective_grant_to_storage).collect())
            .map_err(map_postgres_error)
    }

    async fn groups_with_collection_permission(
        &self,
        query: AuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        groups_can_on_from_backend(
            &self.pool,
            query.collection_id(),
            permission_from_storage(query.permission()),
        )
        .await
        .map(|rows| {
            rows.into_iter()
                .map(authorization_group_to_storage)
                .collect()
        })
        .map_err(map_postgres_error)
    }

    async fn groups_with_collection_permission_page(
        &self,
        query: AuthorizationCollectionGroupsPageQuery,
    ) -> Result<AuthorizationGroupPage, StorageError> {
        let (rows, total) = groups_can_on_paginated_with_total_count_from_backend(
            &self.pool,
            query.groups().collection_id(),
            permission_from_storage(query.groups().permission()),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)?;
        Ok(AuthorizationGroupPage::new(
            rows.into_iter()
                .map(authorization_group_to_storage)
                .collect(),
            total,
        ))
    }

    async fn list_collection_group_permissions(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<Vec<AuthorizationGroupGrant>, StorageError> {
        groups_on_from_backend(
            &self.pool,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
            query
                .required_permissions()
                .iter()
                .copied()
                .map(permission_from_storage)
                .collect(),
            query.query_options().clone(),
        )
        .await
        .map(|rows| {
            rows.into_iter()
                .map(authorization_group_grant_to_storage)
                .collect()
        })
        .map_err(map_postgres_error)
    }

    async fn list_collection_group_permissions_page(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        let (rows, total) = groups_on_paginated_with_total_count_from_backend(
            &self.pool,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
            query
                .required_permissions()
                .iter()
                .copied()
                .map(permission_from_storage)
                .collect(),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)?;
        Ok(AuthorizationGroupGrantPage::new(
            rows.into_iter()
                .map(authorization_group_grant_to_storage)
                .collect(),
            total,
        ))
    }

    async fn collection_group_permission(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<AuthorizationGrant, StorageError> {
        group_on_from_backend(&self.pool, collection_id, group_id)
            .await
            .map(authorization_grant_to_storage)
            .map_err(map_postgres_error)
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
