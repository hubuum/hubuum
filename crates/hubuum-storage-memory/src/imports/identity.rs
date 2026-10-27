use super::*;

impl MemoryStorage {
    pub(super) async fn apply_upsert_identity_scope(
        &self,
        input: StorageImportIdentityScope,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let mut state = self.state.write().await;
        let existing = state.identity_scope_by_name(&parts.name).cloned();
        if let Some(current) = &existing {
            assert_import_revision(parts.condition, current.revision())?;
            if !overwrite {
                return Err(StorageError::conflict(
                    "Import identity scope already exists",
                ));
            }
        } else {
            assert_import_create_condition(parts.condition)?;
        }
        let id = existing.as_ref().map_or_else(
            || {
                let id = IdentityScopeId::new(state.next_identity_scope_id)
                    .expect("memory import identity scope id is positive");
                state.next_identity_scope_id += 1;
                id
            },
            StorageIdentityScope::id,
        );
        let now = Utc::now();
        let (created_at, updated_at) = parts.timestamps.map_or_else(
            || {
                existing
                    .as_ref()
                    .map_or((now, now), |current| (current.created_at(), now))
            },
            StorageImportTimestamps::into_parts,
        );
        let revision = existing
            .as_ref()
            .map_or(Ok(ResourceRevision::INITIAL), |current| {
                current.revision().checked_advance()
            })
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let scope = StorageIdentityScope::try_new(
            id,
            parts.name,
            parts.provider_kind,
            created_at,
            updated_at,
            revision,
        )
        .map_err(invalid_contract_value)?;
        state.store_identity_scope(id.id(), scope);
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::IdentityScope(id));
        }
        Ok(Some(revision))
    }

    pub(super) async fn apply_upsert_group(
        &self,
        input: StorageImportGroup,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let scope_id = self
            .import_identity_scope_id(
                parts.identity_scope_ref.as_deref(),
                parts.identity_scope_key.as_ref(),
                references,
            )
            .await?;
        let mut state = self.state.write().await;
        let existing = state
            .groups
            .values()
            .find(|group| group.identity_scope_id() == scope_id && group.name() == parts.name)
            .cloned();
        if let Some(current) = &existing {
            assert_import_revision(parts.condition, current.revision())?;
            if !overwrite {
                return Err(StorageError::conflict("Import group already exists"));
            }
        } else {
            assert_import_create_condition(parts.condition)?;
        }
        let id = existing.as_ref().map_or_else(
            || {
                let id =
                    GroupId::new(state.next_group_id).expect("memory import group id is positive");
                state.next_group_id += 1;
                id
            },
            StorageIdentityGroup::id,
        );
        let now = Utc::now();
        let (created_at, updated_at) = parts.timestamps.map_or_else(
            || {
                existing
                    .as_ref()
                    .map_or((now, now), |value| (value.created_at(), now))
            },
            StorageImportTimestamps::into_parts,
        );
        let revision = existing
            .as_ref()
            .map_or(Ok(ResourceRevision::INITIAL), |value| {
                value.revision().checked_advance()
            })
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).expect("group id is a valid resource id"),
            created_at,
            updated_at,
            revision,
        )
        .map_err(invalid_contract_value)?;
        let group = StorageIdentityGroup::builder(
            metadata,
            parts.name,
            parts.description,
            scope_id,
            parts.managed_by,
        )
        .external_key(parts.external_key)
        .last_sync_attempted_at(parts.last_sync_attempted_at)
        .last_sync_success_at(parts.last_sync_success_at)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.store_group(id.id(), group);
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Group(id));
        }
        Ok(Some(revision))
    }

    pub(super) async fn apply_upsert_group_membership(
        &self,
        input: StorageImportGroupMembership,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let principal_id = self
            .import_principal_id(
                parts.principal_ref.as_deref(),
                parts.principal_key.as_ref(),
                references,
            )
            .await?;
        let group_id = self
            .import_group_id(
                parts.group_ref.as_deref(),
                parts.group_key.as_ref(),
                references,
            )
            .await?;
        let mut state = self.state.write().await;
        let existing = state
            .memberships
            .get(&(principal_id.id(), group_id.id()))
            .cloned();
        if let Some(current) = &existing {
            assert_import_revision(parts.condition, current.revision())?;
            if !overwrite {
                return Err(StorageError::conflict("Import membership already exists"));
            }
        } else {
            assert_import_create_condition(parts.condition)?;
        }
        let now = Utc::now();
        let (created_at, updated_at) = parts.timestamps.map_or_else(
            || {
                existing
                    .as_ref()
                    .map_or((now, now), |value| (value.created_at(), now))
            },
            StorageImportTimestamps::into_parts,
        );
        let revision = existing
            .as_ref()
            .map_or(Ok(ResourceRevision::INITIAL), |value| {
                value.revision().checked_advance()
            })
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let membership = StoragePrincipalGroup::try_new(
            principal_id,
            group_id,
            created_at,
            updated_at,
            revision,
        )
        .map_err(invalid_contract_value)?;
        state.store_membership((principal_id.id(), group_id.id()), membership);
        if parts.sources.is_empty() {
            state
                .external_memberships
                .remove(&(principal_id.id(), group_id.id()));
        } else {
            state
                .external_memberships
                .insert((principal_id.id(), group_id.id()));
        }
        Ok(Some(revision))
    }
}
