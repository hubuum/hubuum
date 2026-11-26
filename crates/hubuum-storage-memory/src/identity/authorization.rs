use super::*;
use hubuum_storage_core::{StorageAuthorizationResource, StorageAuthorizationResourcesQuery};

#[async_trait]
impl AuthorizationDataStorage for MemoryStorage {
    async fn load_authorization_resources(
        &self,
        query: StorageAuthorizationResourcesQuery,
    ) -> Result<Vec<StorageAuthorizationResource>, StorageError> {
        use StorageAuthorizationResource as R;
        use hubuum_storage_core::StorageAuthorizationResourceKey as K;
        let state = self.state.read().await;
        let class = |id: ClassId| {
            state
                .classes
                .get(&id.id())
                .map(|c| StorageAuthorizationClassResource::new(c.id(), c.collection_id()))
        };
        let object = |id: ObjectId| {
            state.objects.get(&id.id()).map(|o| {
                StorageAuthorizationObjectResource::new(
                    o.id(),
                    o.collection_id(),
                    o.class_id(),
                    o.name(),
                )
            })
        };
        Ok(query
            .keys()
            .iter()
            .filter_map(|key| {
                Some(match key {
                    K::Class(id) => {
                        let c = state.classes.get(&id.id())?;
                        R::Class {
                            resource: class(*id)?,
                            name: c.name().into(),
                        }
                    }
                    K::Object(id) => R::Object(object(*id)?),
                    K::Collection(id) => R::Collection {
                        id: *id,
                        name: state.collections.get(&id.id())?.name().into(),
                    },
                    K::ExportTemplate(id) => {
                        let t = state.export_templates.get(&id.id())?;
                        R::ExportTemplate {
                            id: *id,
                            collection_id: t.collection_id(),
                            name: t.name().into(),
                        }
                    }
                    K::RemoteTarget(id) => {
                        let t = state.remote_targets.get(&id.id())?;
                        R::RemoteTarget {
                            id: *id,
                            collection_id: t.collection_id(),
                            name: t.name().into(),
                        }
                    }
                    K::ClassRelation(id) => {
                        let r = state.class_relations.get(&id.id())?;
                        R::ClassRelation {
                            id: *id,
                            from: class(r.from_class_id())?,
                            to: class(r.to_class_id())?,
                        }
                    }
                    K::ObjectRelation(id) => {
                        let r = state.object_relations.get(&id.id())?;
                        R::ObjectRelation {
                            id: *id,
                            from: object(r.from_object_id())?,
                            to: object(r.to_object_id())?,
                            class_relation_id: r.class_relation_id(),
                        }
                    }
                })
            })
            .collect())
    }

    async fn get_authorization_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthorizationPrincipal, StorageError> {
        let state = self.state.read().await;
        if !state.principals.contains_key(&principal_id.id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                principal_id.id()
            )));
        }
        Ok(StorageAuthorizationPrincipal::new(
            principal_id,
            principal_group_ids(&state, principal_id),
        ))
    }

    async fn is_authorization_principal_group_member(
        &self,
        query: StorageAuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(state.groups.values().any(|group| {
            group.name() == query.group_name()
                && state
                    .identity_scopes
                    .get(&group.identity_scope_id().id())
                    .is_some_and(|scope| scope.name() == query.identity_scope())
                && state
                    .memberships
                    .contains_key(&(query.principal_id().id(), group.id().id()))
        }))
    }

    async fn list_authorization_classes(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationClassResource>, StorageError> {
        let state = self.state.read().await;
        Ok(query
            .ids()
            .iter()
            .filter_map(|id| state.classes.get(&id.id()))
            .map(|class| StorageAuthorizationClassResource::new(class.id(), class.collection_id()))
            .collect())
    }

    async fn list_authorization_objects(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationObjectResource>, StorageError> {
        let state = self.state.read().await;
        Ok(query
            .ids()
            .iter()
            .filter_map(|id| state.objects.get(&id.id()))
            .map(|object| {
                StorageAuthorizationObjectResource::new(
                    object.id(),
                    object.collection_id(),
                    object.class_id(),
                    object.name(),
                )
            })
            .collect())
    }

    async fn authorize_local_collection_batch(
        &self,
        queries: Vec<StorageAuthorizationCollectionAccessQuery>,
    ) -> Result<Vec<bool>, StorageError> {
        let state = self.state.read().await;
        Ok(queries
            .iter()
            .map(|query| {
                principal_has_collection_permissions(
                    &state,
                    query.principal_id(),
                    query.collection_id(),
                    query.permissions(),
                )
            })
            .collect())
    }

    async fn authorize_local_collection(
        &self,
        query: StorageAuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(principal_has_collection_permissions(
            &state,
            query.principal_id(),
            query.collection_id(),
            query.permissions(),
        ))
    }

    async fn authorize_local_collections(
        &self,
        query: StorageAuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(query.collection_ids().iter().all(|collection_id| {
            principal_has_collection_permissions(
                &state,
                query.principal_id(),
                *collection_id,
                query.permissions(),
            )
        }))
    }

    async fn list_local_authorized_collections(
        &self,
        query: StorageAuthorizationCollectionsQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        let state = self.state.read().await;
        state
            .collections
            .values()
            .filter(|collection| {
                principal_has_collection_permissions(
                    &state,
                    query.principal_id(),
                    collection.id(),
                    query.permissions(),
                )
            })
            .map(authorization_collection)
            .collect()
    }

    async fn load_authorization_collection_candidates(
        &self,
        query: StorageAuthorizationCollectionCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationCollection>, StorageError> {
        let state = self.state.read().await;
        let limit = query.page_limit();
        let mut rows = state
            .collections
            .values()
            .filter(|collection| query.after_id().is_none_or(|id| collection.id() > id))
            .take(limit.get().saturating_add(1))
            .map(authorization_collection)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = rows.len() > limit.get();
        rows.truncate(limit.get());
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn load_authorization_group_candidates(
        &self,
        query: StorageAuthorizationGroupCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationGroup>, StorageError> {
        let filters = ResourceFilters::for_groups(query.options())?;
        let state = self.state.read().await;
        let limit = query.page_limit();
        let mut rows = state
            .groups
            .values()
            .filter(|group| {
                filters.matches_with_identity_scope(
                    group.id().id(),
                    group.name(),
                    group.description(),
                    state
                        .identity_scopes
                        .get(&group.identity_scope_id().id())
                        .map(|scope| scope.name()),
                )
            })
            .take(limit.get().saturating_add(1))
            .map(authorization_group)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = rows.len() > limit.get();
        rows.truncate(limit.get());
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .values()
            .map(|grant| authorization_policy_row(&state, grant))
            .collect()
    }

    async fn list_local_collection_grants(
        &self,
        query: StorageAuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && permissions_include(grant.permissions(), query.required_permissions())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }

    async fn get_local_collection_grant(
        &self,
        key: StorageAuthorizationGrantKey,
    ) -> Result<Option<StorageAuthorizationGrant>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .authorization_grants
            .get(&(key.collection_id().id(), key.group_id().id()))
            .cloned())
    }

    async fn get_local_collection_permission_set(
        &self,
        query: StorageAuthorizationPermissionSetQuery,
    ) -> Result<StorageAuthorizationPermissionSet, StorageError> {
        let state = self.state.read().await;
        let collection = state
            .collections
            .get(&query.collection_id().id())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Collection {} was not found",
                    query.collection_id().id()
                ))
            })?;
        let grants = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && query.group_id().is_none_or(|id| grant.group_id() == id)
            })
            .cloned()
            .collect();
        StorageAuthorizationPermissionSet::try_new(
            collection.id(),
            state
                .authorization_revisions
                .get(&collection.id().id())
                .copied()
                .unwrap_or(ResourceRevision::INITIAL),
            grants,
        )
        .map_err(invalid_contract_value)
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
        let key = mutation.key();
        let mut state = self.state.write().await;
        if !state.collections.contains_key(&key.collection_id().id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                key.collection_id().id()
            )));
        }
        if !state.groups.contains_key(&key.group_id().id()) {
            return Err(StorageError::not_found(format!(
                "Group {} was not found",
                key.group_id().id()
            )));
        }
        let map_key = (key.collection_id().id(), key.group_id().id());
        let current = state.authorization_grants.get(&map_key).cloned();
        let mut permissions = if mutation.replace_existing() {
            Vec::new()
        } else {
            current
                .as_ref()
                .map(|grant| grant.permissions().to_vec())
                .unwrap_or_default()
        };
        permissions.extend_from_slice(mutation.permissions());
        permissions.sort_unstable();
        permissions.dedup();
        if current
            .as_ref()
            .is_some_and(|grant| grant.permissions() == permissions)
        {
            return Ok(StorageMutationOutcome::unchanged(
                current.expect("grant exists"),
            ));
        }
        let now = Utc::now();
        let (id, created_at) = match &current {
            Some(grant) => (grant.id(), grant.created_at()),
            None => (
                AuthorizationGrantId::new(state.authorization_grant_ids.allocate()?)
                    .expect("allocated id is positive"),
                now,
            ),
        };
        let grant = StorageAuthorizationGrant::try_new(
            id,
            key.collection_id(),
            key.group_id(),
            permissions,
            created_at,
            now,
        )
        .map_err(invalid_contract_value)?;
        let revision = state
            .authorization_revisions
            .get(&key.collection_id().id())
            .copied()
            .unwrap_or(ResourceRevision::INITIAL)
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state
            .authorization_revisions
            .insert(key.collection_id().id(), revision);
        state.authorization_grants.insert(map_key, grant.clone());
        let receipt = state.append_simple_event(
            EntityType::Permission,
            id.id(),
            None,
            Action::Granted,
            mutation.event_context(),
            format!(
                "Collection {} permissions granted to group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(grant, receipt))
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
        let key = mutation.key();
        let mut state = self.state.write().await;
        let map_key = (key.collection_id().id(), key.group_id().id());
        let Some(current) = state.authorization_grants.get(&map_key).cloned() else {
            return Err(StorageError::not_found(format!(
                "Collection {} has no grant for group {}",
                key.collection_id().id(),
                key.group_id().id()
            )));
        };
        let permissions = current
            .permissions()
            .iter()
            .copied()
            .filter(|permission| !mutation.permissions().contains(permission))
            .collect::<Vec<_>>();
        if permissions == current.permissions() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let grant = StorageAuthorizationGrant::try_new(
            current.id(),
            current.collection_id(),
            current.group_id(),
            permissions,
            current.created_at(),
            Utc::now(),
        )
        .map_err(invalid_contract_value)?;
        let revision = state
            .authorization_revisions
            .get(&key.collection_id().id())
            .copied()
            .unwrap_or(ResourceRevision::INITIAL)
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state
            .authorization_revisions
            .insert(key.collection_id().id(), revision);
        state.authorization_grants.insert(map_key, grant.clone());
        let receipt = state.append_simple_event(
            EntityType::Permission,
            grant.id().id(),
            None,
            Action::Revoked,
            mutation.event_context(),
            format!(
                "Collection {} permissions revoked from group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(grant, receipt))
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: StorageAuthorizationGrantDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let key = request.key();
        let mut state = self.state.write().await;
        let Some(grant) = state
            .authorization_grants
            .remove(&(key.collection_id().id(), key.group_id().id()))
        else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let receipt = state.append_simple_event(
            EntityType::Permission,
            grant.id().id(),
            None,
            Action::Revoked,
            request.event_context(),
            format!(
                "All collection {} permissions revoked from group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}
#[async_trait]
impl CollectionAuthorizationQueryStorage for MemoryStorage {
    async fn load_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal_id());
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect()
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, principal_id);
        state
            .authorization_grants
            .values()
            .filter(|grant| group_ids.contains(&grant.group_id()))
            .map(|grant| authorization_policy_row(&state, grant))
            .collect()
    }

    async fn list_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal().principal_id());
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.principal().collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal_id());
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_effective_group_grant(&state, grant))
            .collect()
    }

    async fn list_visible_collections(
        &self,
        query: StorageAuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        let (principal_id, is_admin, permission, scope) = query.into_parts();
        let (scope_permissions, scope_resources) = scope
            .map(StorageAuthenticationTokenScope::into_parts)
            .unwrap_or((None, None));
        if scope_permissions
            .as_ref()
            .is_some_and(|permissions| !permissions.contains(&permission))
        {
            return Ok(Vec::new());
        }
        let resources = scope_resources.map(StorageAuthenticationResourceScope::into_parts);
        let state = self.state.read().await;
        state
            .collections
            .values()
            .filter(|collection| {
                let resource_allowed =
                    resources
                        .as_ref()
                        .is_none_or(|(collection_ids, class_ids, object_ids)| {
                            collection_ids.contains(&collection.id())
                                || class_ids.iter().any(|class_id| {
                                    state.classes.get(&class_id.id()).is_some_and(|class| {
                                        class.collection_id() == collection.id()
                                    })
                                })
                                || object_ids.iter().any(|object_id| {
                                    state.objects.get(&object_id.id()).is_some_and(|object| {
                                        object.collection_id() == collection.id()
                                    })
                                })
                        });
                resource_allowed
                    && (is_admin
                        || principal_has_collection_permissions(
                            &state,
                            principal_id,
                            collection.id(),
                            &[permission],
                        ))
            })
            .map(authorization_collection)
            .collect()
    }

    async fn has_group_collection_permission(
        &self,
        query: StorageAuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .authorization_grants
            .get(&(query.collection_id().id(), query.group_id().id()))
            .is_some_and(|grant| grant.permissions().contains(&query.permission())))
    }

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .get(&(collection_id.id(), group_id.id()))
            .into_iter()
            .map(|grant| authorization_effective_group_grant(&state, grant))
            .collect()
    }

    async fn load_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<StorageAuthorizationGroup>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && grant.permissions().contains(&query.permission())
            })
            .map(|grant| {
                state
                    .groups
                    .get(&grant.group_id().id())
                    .ok_or_else(|| StorageError::internal("authorization grant group is missing"))
                    .and_then(authorization_group)
            })
            .collect()
    }

    async fn list_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroup>, StorageError> {
        let filters = ResourceFilters::for_groups(query.query_options())?;
        let state = self.state.read().await;
        let groups = query.groups();
        let mut rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == groups.collection_id()
                    && grant.permissions().contains(&groups.permission())
            })
            .map(|grant| {
                state
                    .groups
                    .get(&grant.group_id().id())
                    .ok_or_else(|| StorageError::internal("authorization grant group is missing"))
                    .and_then(authorization_group)
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.retain(|group| {
            filters.matches(group.id().id(), group.group_name(), group.description())
        });
        page(rows, query.query_options())
    }
}
