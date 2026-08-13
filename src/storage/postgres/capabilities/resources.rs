use super::super::*;

#[async_trait]
impl GroupStorage for PostgresStorage {
    async fn load_group(&self, group_id: i32) -> Result<Group, StorageError> {
        GroupID::new(group_id)
            .map_err(map_postgres_error)?
            .load_group_record(&self.pool)
            .await
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
        command: &NewGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError> {
        command
            .save_group_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_group(
        &self,
        group_id: i32,
        update: &UpdateGroup,
        context: Option<&EventContext>,
    ) -> Result<Group, StorageError> {
        let group_id = GroupID::new(group_id).map_err(map_postgres_error)?;
        update
            .update_group_record(group_id.id(), &self.pool, context)
            .await
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

    async fn group_members(&self, group_id: i32) -> Result<Vec<Principal>, StorageError> {
        let group = self.load_group(group_id).await?;
        group
            .load_group_members(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_members_page(
        &self,
        group_id: i32,
        query_options: &QueryOptions,
    ) -> Result<Vec<(PrincipalGroup, Principal)>, StorageError> {
        let group = self.load_group(group_id).await?;
        group
            .load_group_members_paginated(&self.pool, query_options)
            .await
            .map_err(map_postgres_error)
    }

    async fn count_group_members(
        &self,
        group_id: i32,
        query_options: &QueryOptions,
    ) -> Result<i64, StorageError> {
        let group = self.load_group(group_id).await?;
        group
            .count_group_members_paginated(&self.pool, query_options)
            .await
            .map_err(map_postgres_error)
    }

    async fn group_member_principal(&self, principal_id: i32) -> Result<Principal, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::group::group_member_principal(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn add_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<PrincipalGroup, StorageError> {
        GroupID::new(group_id).map_err(map_postgres_error)?;
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::group::save_manual_membership(&self.pool, principal_id, group_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn remove_group_member(
        &self,
        principal_id: i32,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        let group = self.load_group(group_id).await?;
        group
            .remove_group_member_from_backend(principal_id, &self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl PrincipalStorage for PostgresStorage {
    async fn load_principal(&self, principal_id: i32) -> Result<Principal, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::load_principal_by_id(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_principal_settings(
        &self,
        principal_id: i32,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::load_principal_settings(&self.pool, principal_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn replace_principal_settings(
        &self,
        principal_id: i32,
        settings: PrincipalSettings,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::mutate_principal_settings(
            &self.pool,
            principal_id,
            operations::principal::PrincipalSettingsMutation::Replace,
            settings,
            context,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn merge_principal_settings(
        &self,
        principal_id: i32,
        patch: PrincipalSettings,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::mutate_principal_settings(
            &self.pool,
            principal_id,
            operations::principal::PrincipalSettingsMutation::Patch,
            patch,
            context,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn apply_principal_settings_patch(
        &self,
        principal_id: i32,
        patch: PrincipalSettingsPatch,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::apply_principal_settings_patch(
            &self.pool,
            principal_id,
            patch,
            context,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn reset_principal_settings(
        &self,
        principal_id: i32,
        context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, StorageError> {
        PrincipalID::new(principal_id).map_err(map_postgres_error)?;
        operations::principal::mutate_principal_settings(
            &self.pool,
            principal_id,
            operations::principal::PrincipalSettingsMutation::Reset,
            PrincipalSettings::default(),
            context,
        )
        .await
        .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CollectionRecordStorage for PostgresStorage {
    async fn create_collection_record(
        &self,
        command: &NewCollectionWithAssignee,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        command
            .save_collection_with_assignee_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_collection_record(
        &self,
        update: &UpdateCollection,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        update
            .update_collection_record(&self.pool, collection_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_collection_record(
        &self,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        CollectionID::new(collection_id)
            .map_err(map_postgres_error)?
            .delete_collection_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn move_collection_record(
        &self,
        collection_id: i32,
        new_parent_collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, StorageError> {
        move_collection_record_from_backend(
            &self.pool,
            collection_id,
            new_parent_collection_id,
            context,
        )
        .await
        .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CollectionPermissionStorage for PostgresStorage {
    async fn principal_collection_permissions(
        &self,
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<GroupPermission>, StorageError> {
        principal_on_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn principal_all_collection_permissions(
        &self,
        principal_id: i32,
    ) -> Result<Vec<(Collection, Group, Permission)>, StorageError> {
        principal_all_permissions_from_backend(
            &self.pool,
            PrincipalID::new(principal_id).map_err(map_postgres_error)?,
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn principal_collection_permissions_page(
        &self,
        query: CollectionPrincipalPageQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError> {
        principal_on_paginated_with_total_count_from_backend(
            &self.pool,
            PrincipalID::new(query.principal().principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.principal().collection_id())
                .map_err(map_postgres_error)?
                .id(),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn effective_principal_collection_permissions(
        &self,
        query: CollectionPrincipalQuery,
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError> {
        effective_principal_on_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn visible_collections(
        &self,
        query: CollectionVisibilityQuery,
    ) -> Result<Vec<Collection>, StorageError> {
        user_can_on_any_from_backend(
            &self.pool,
            PrincipalID::new(query.principal_id()).map_err(map_postgres_error)?,
            query.permission(),
            query.scopes(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn group_has_collection_permission(
        &self,
        query: CollectionGroupPermissionQuery,
    ) -> Result<bool, StorageError> {
        group_can_on_from_backend(
            &self.pool,
            query.group_id(),
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
            query.permission(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn effective_group_collection_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Vec<EffectiveGroupPermission>, StorageError> {
        effective_group_on_from_backend(&self.pool, collection_id, group_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn groups_with_collection_permission(
        &self,
        query: CollectionGroupsQuery,
    ) -> Result<Vec<Group>, StorageError> {
        groups_can_on_from_backend(&self.pool, query.collection_id(), query.permission())
            .await
            .map_err(map_postgres_error)
    }

    async fn groups_with_collection_permission_page(
        &self,
        query: CollectionGroupsPageQuery,
    ) -> Result<(Vec<Group>, i64), StorageError> {
        groups_can_on_paginated_with_total_count_from_backend(
            &self.pool,
            query.groups().collection_id(),
            query.groups().permission(),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn list_collection_group_permissions(
        &self,
        query: CollectionGrantListQuery,
    ) -> Result<Vec<GroupPermission>, StorageError> {
        groups_on_from_backend(
            &self.pool,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
            query.permissions().to_vec(),
            query.query_options().clone(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn list_collection_group_permissions_page(
        &self,
        query: CollectionGrantListQuery,
    ) -> Result<(Vec<GroupPermission>, i64), StorageError> {
        groups_on_paginated_with_total_count_from_backend(
            &self.pool,
            CollectionID::new(query.collection_id())
                .map_err(map_postgres_error)?
                .id(),
            query.permissions().to_vec(),
            query.query_options(),
        )
        .await
        .map_err(map_postgres_error)
    }

    async fn collection_group_permission(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Permission, StorageError> {
        group_on_from_backend(&self.pool, collection_id, group_id)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CollectionStore for PostgresStorage {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError> {
        id.collection_from_backend(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        command
            .save_collection_with_assignee_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn update_collection(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        changes
            .update_collection_record(&self.pool, id.id(), Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        id.delete_collection_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError> {
        collection_children_from_backend(&self.pool, id.id())
            .await
            .map_err(map_postgres_error)
    }

    async fn collection_ancestors(
        &self,
        id: CollectionID,
    ) -> Result<Vec<Collection>, StorageError> {
        collection_ancestors_from_backend(&self.pool, id.id())
            .await
            .map_err(map_postgres_error)
    }

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError> {
        move_collection_record_from_backend(&self.pool, id.id(), new_parent_id.id(), Some(context))
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ClassRecordStorage for PostgresStorage {
    async fn create_class_record(
        &self,
        class: &NewHubuumClass,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError> {
        class.validate_schema().map_err(map_postgres_error)?;
        class
            .create_class_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_class_record(
        &self,
        update: &UpdateHubuumClass,
        class_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, StorageError> {
        update
            .update_class_record(&self.pool, class_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_class_record(
        &self,
        class: &HubuumClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        class
            .delete_class_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_class_record(&self, class_id: i32) -> Result<HubuumClass, StorageError> {
        HubuumClassID::new(class_id)
            .map_err(map_postgres_error)?
            .load_class_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_collection(&self, class_id: i32) -> Result<Collection, StorageError> {
        HubuumClassID::new(class_id)
            .map_err(map_postgres_error)?
            .lookup_class_collection(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_names(
        &self,
        class_ids: &ClassIdSet,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        load_class_names(&self.pool, class_ids)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ClassStore for PostgresStorage {
    async fn resolve_class(
        &self,
        selector: ClassSelector,
    ) -> Result<ResolvedClassTarget, StorageError> {
        let class = selector
            .resolve_class_selector_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        Ok(ResolvedClassTarget::new(selector, class))
    }

    async fn create_class(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        command.validate_schema().map_err(map_postgres_error)?;
        command
            .create_class_record(&self.pool, Some(context))
            .await
            .map_err(map_postgres_error)
    }

    async fn update_class(
        &self,
        target: &ResolvedClassTarget,
        changes: UpdateHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, StorageError> {
        changes
            .update_resolved_class_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_class(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ClassRelationStore for PostgresStorage {
    async fn prepare_class_relation(
        &self,
        command: NewHubuumClassRelation,
    ) -> Result<PreparedClassRelation, StorageError> {
        command
            .prepare_class_relation_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn resolve_class_relation(
        &self,
        id: HubuumClassRelationID,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        id.resolve_class_relation_target_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_class_relation(
        &self,
        prepared: &PreparedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<ResolvedClassRelationTarget, StorageError> {
        let relation = prepared
            .create_prepared_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)?;
        ResolvedClassRelationTarget::new(
            relation,
            prepared.from_class().clone(),
            prepared.to_class().clone(),
        )
        .map_err(map_postgres_error)
    }

    async fn delete_class_relation(
        &self,
        target: &ResolvedClassRelationTarget,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_class_relation_from_command(
        &self,
        command: NewHubuumClassRelation,
        context: Option<&EventContext>,
    ) -> Result<HubuumClassRelation, StorageError> {
        command
            .save_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_class_relation_by_id(
        &self,
        id: HubuumClassRelationID,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        id.delete_class_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectRelationStore for PostgresStorage {
    async fn prepare_object_relation(
        &self,
        selector: ObjectRelationCreateSelector,
    ) -> Result<PreparedObjectRelation, StorageError> {
        selector
            .prepare_object_relation_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn resolve_object_relation(
        &self,
        selector: ObjectRelationSelector,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        selector
            .resolve_object_relation_target_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_object_relation(
        &self,
        prepared: &PreparedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<ResolvedObjectRelationTarget, StorageError> {
        let relation = prepared
            .create_prepared_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)?;
        ResolvedObjectRelationTarget::new(
            relation,
            prepared.from_object().clone(),
            prepared.to_object().clone(),
            prepared.class_relation().clone(),
        )
        .map_err(map_postgres_error)
    }

    async fn delete_object_relation(
        &self,
        target: &ResolvedObjectRelationTarget,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_object_relation_from_command(
        &self,
        command: NewHubuumObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<HubuumObjectRelation, StorageError> {
        command
            .save_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_object_relation_by_id(
        &self,
        id: HubuumObjectRelationID,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        id.delete_object_relation_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectStore for PostgresStorage {
    async fn resolve_object(
        &self,
        selector: ObjectSelector,
    ) -> Result<ResolvedObjectTarget, StorageError> {
        let (class, object) = selector
            .resolve_object_selector_record(&self.pool)
            .await
            .map_err(map_postgres_error)?;
        Ok(ResolvedObjectTarget::new(selector, class, object))
    }

    async fn create_object(
        &self,
        class: &ResolvedClassTarget,
        command: NewHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        command
            .create_object_in_resolved_class_record(&self.pool, class, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_object(
        &self,
        target: &ResolvedObjectTarget,
        changes: UpdateHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        changes
            .update_resolved_object_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn patch_object_data(
        &self,
        target: &ResolvedObjectTarget,
        patch: ObjectDataPatchDocument,
        context: &EventContext,
    ) -> Result<HubuumObject, StorageError> {
        patch
            .patch_object_data_record(&self.pool, target, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_object(
        &self,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        target
            .delete_resolved_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectRecordStorage for PostgresStorage {
    async fn validate_object(&self, object: &HubuumObject) -> Result<(), StorageError> {
        object
            .validate_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn validate_new_object(&self, object: &NewHubuumObject) -> Result<(), StorageError> {
        object
            .validate_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn validate_object_update(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
    ) -> Result<(), StorageError> {
        (update, object_id)
            .validate_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn save_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        object
            .save_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_object_record(
        &self,
        object: &NewHubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        object
            .save_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn update_object_record(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        update
            .update_object_record(&self.pool, object_id, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn delete_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        object
            .delete_object_record(&self.pool, context)
            .await
            .map_err(map_postgres_error)
    }

    async fn load_object_record(&self, object_id: i32) -> Result<HubuumObject, StorageError> {
        HubuumObjectID::new(object_id)
            .map_err(map_postgres_error)?
            .load_object_record(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_collection(&self, object_id: i32) -> Result<Collection, StorageError> {
        HubuumObjectID::new(object_id)
            .map_err(map_postgres_error)?
            .lookup_object_collection(&self.pool)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_class(&self, object_id: i32) -> Result<HubuumClass, StorageError> {
        HubuumObjectID::new(object_id)
            .map_err(map_postgres_error)?
            .lookup_object_class(&self.pool)
            .await
            .map_err(map_postgres_error)
    }
}
