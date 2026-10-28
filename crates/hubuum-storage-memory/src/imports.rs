mod identity;
mod integrations;
mod relations;
mod resources;

use super::*;

fn assert_import_revision(
    condition: Option<StorageImportWriteCondition>,
    current_revision: ResourceRevision,
) -> Result<(), StorageError> {
    let Some(expected) = condition.and_then(StorageImportWriteCondition::expected_revision) else {
        return Ok(());
    };
    if expected == current_revision.get() {
        return Ok(());
    }
    Err(StorageError::precondition_failed(
        format!(
            "stale_revision: expected revision {expected}, observed {}",
            current_revision.get()
        ),
        Some(current_revision),
    ))
}

fn assert_import_create_condition(
    condition: Option<StorageImportWriteCondition>,
) -> Result<(), StorageError> {
    if condition.is_some_and(StorageImportWriteCondition::requires_existing) {
        return Err(StorageError::precondition_failed(
            "conditional_import_target_missing",
            None,
        ));
    }
    Ok(())
}

impl MemoryStorage {
    pub(crate) async fn commit_import_operation(
        &self,
        operation: StorageImportOperation,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        // Hold the live state lock until the complete item is published so
        // concurrent writes cannot be lost when replacing the staged state.
        let mut state = self.state.write().await;
        let scratch = Self {
            schema_limits: self.schema_limits,
            state: Arc::new(MemoryStateLock::new(state.clone())),
        };
        let mut next_references = references.clone();
        let revision = scratch
            .apply_import_operation(operation, &mut next_references)
            .await?;
        *state = scratch.state.read().await.clone();
        *references = next_references;
        Ok(revision)
    }

    async fn import_identity_scope_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportIdentityScopeKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<IdentityScopeId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::IdentityScope(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import identity-scope reference '{reference}' was not found"
                ))),
            };
        }
        let name = key
            .ok_or_else(|| {
                StorageError::invalid_input("Import identity scope selector is missing")
            })?
            .clone()
            .into_parts()
            .name;
        self.state
            .read()
            .await
            .identity_scope_by_name(&name)
            .map(StorageIdentityScope::id)
            .ok_or_else(|| StorageError::not_found("Import identity scope was not found"))
    }

    async fn import_group_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportGroupKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<GroupId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Group(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import group reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import group selector is missing"))?
            .clone()
            .into_parts();
        let state = self.state.read().await;
        state
            .groups
            .values()
            .find(|group| {
                group.name() == parts.name
                    && state
                        .identity_scopes
                        .get(&group.identity_scope_id().id())
                        .is_some_and(|scope| scope.name() == parts.identity_scope)
            })
            .map(StorageIdentityGroup::id)
            .ok_or_else(|| StorageError::not_found("Import group was not found"))
    }

    async fn import_principal_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportPrincipalKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<PrincipalId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Principal(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import principal reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import principal selector is missing"))?
            .clone()
            .into_parts();
        let state = self.state.read().await;
        state
            .principals
            .values()
            .find(|principal| {
                principal.name() == parts.name
                    && state
                        .identity_scopes
                        .get(&principal.identity_scope_id().id())
                        .is_some_and(|scope| scope.name() == parts.identity_scope)
            })
            .map(StoragePrincipal::id)
            .ok_or_else(|| StorageError::not_found("Import principal was not found"))
    }

    async fn import_collection_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportCollectionKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<CollectionId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Collection(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import collection reference '{reference}' was not found"
                ))),
            };
        }
        let key = key
            .ok_or_else(|| StorageError::invalid_input("Import collection selector is missing"))?;
        self.get_import_collection_by_key(key)
            .await?
            .map(|collection| collection.id())
            .ok_or_else(|| StorageError::not_found("Import collection was not found"))
    }

    async fn import_class_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportClassKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<ClassId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Class(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import class reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import class selector is missing"))?
            .clone()
            .into_parts();
        let collection_id = self
            .import_collection_id(
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
                references,
            )
            .await?;
        self.get_import_class_by_name(collection_id, &parts.name)
            .await?
            .map(|class| class.id())
            .ok_or_else(|| StorageError::not_found("Import class was not found"))
    }

    async fn import_object_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportObjectKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<ObjectId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Object(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import object reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import object selector is missing"))?
            .clone()
            .into_parts();
        let class_id = self
            .import_class_id(
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
                references,
            )
            .await?;
        self.get_import_object_by_name(class_id, &parts.name)
            .await?
            .map(|object| object.id())
            .ok_or_else(|| StorageError::not_found("Import object was not found"))
    }

    async fn import_event_sink_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportEventSinkKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<EventSinkId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::EventSink(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import event-sink reference '{reference}' was not found"
                ))),
            };
        }
        let name = key
            .ok_or_else(|| StorageError::invalid_input("Import event-sink selector is missing"))?
            .clone()
            .into_parts()
            .name;
        self.state
            .read()
            .await
            .event_sinks
            .values()
            .find(|sink| sink.name() == name && sink.collection_id().is_none())
            .map(StorageEventSink::id)
            .ok_or_else(|| StorageError::not_found("Import event sink was not found"))
    }

    pub(crate) async fn apply_import_operation(
        &self,
        operation: StorageImportOperation,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        match operation {
            StorageImportOperation::UpsertIdentityScope { input, overwrite } => {
                self.apply_upsert_identity_scope(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertGroup { input, overwrite } => {
                self.apply_upsert_group(input, overwrite, references).await
            }
            StorageImportOperation::UpsertPrincipal { input, overwrite } => {
                self.apply_import_principal(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertGroupMembership { input, overwrite } => {
                self.apply_upsert_group_membership(input, overwrite, references)
                    .await
            }
            StorageImportOperation::CreateCollection(input) => {
                self.apply_create_collection(input, references).await
            }
            StorageImportOperation::CreateClass(input) => {
                self.apply_create_class(input, references).await
            }
            StorageImportOperation::UpdateCollection {
                collection_id,
                input,
            } => {
                self.apply_update_collection(collection_id, input, references)
                    .await
            }
            StorageImportOperation::UpdateClass { class_id, input } => {
                self.apply_update_class(class_id, input, references).await
            }
            StorageImportOperation::CreateObject(input) => {
                self.apply_create_object(input, references).await
            }
            StorageImportOperation::UpdateObject { object_id, input } => {
                self.apply_update_object(object_id, input, references).await
            }
            StorageImportOperation::CreateClassRelation(input) => {
                self.apply_create_class_relation(input, references).await
            }
            StorageImportOperation::CheckClassRelationCondition(input) => {
                self.apply_check_class_relation_condition(input, references)
                    .await
            }
            StorageImportOperation::UpdateClassRelationTimestamps { input, timestamps } => {
                self.apply_update_class_relation_timestamps(input, timestamps, references)
                    .await
            }
            StorageImportOperation::CreateObjectRelation(input) => {
                self.apply_create_object_relation(input, references).await
            }
            StorageImportOperation::CheckObjectRelationCondition(input) => {
                self.apply_check_object_relation_condition(input, references)
                    .await
            }
            StorageImportOperation::UpdateObjectRelationTimestamps { input, timestamps } => {
                self.apply_update_object_relation_timestamps(input, timestamps, references)
                    .await
            }
            StorageImportOperation::UpsertComputedField { input, overwrite } => {
                self.apply_upsert_computed_field(input, overwrite, references)
                    .await
            }
            StorageImportOperation::ApplyCollectionPermissions { input, overwrite } => {
                self.apply_apply_collection_permissions(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertExportTemplate { input, overwrite } => {
                self.apply_upsert_export_template(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertRemoteTarget { input, overwrite } => {
                self.apply_upsert_remote_target(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertEventSink { input, overwrite } => {
                self.apply_upsert_event_sink(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertEventSubscription { input, overwrite } => {
                self.apply_upsert_event_subscription(input, overwrite, references)
                    .await
            }
        }
    }

    async fn import_class_relation(
        &self,
        first: ClassId,
        second: ClassId,
    ) -> Result<StorageClassRelation, StorageError> {
        let (from, to) = ordered_ids(first, second);
        self.state
            .read()
            .await
            .class_relations
            .values()
            .find(|relation| relation.from_class_id() == from && relation.to_class_id() == to)
            .cloned()
            .ok_or_else(|| StorageError::not_found("Import class relation was not found"))
    }

    async fn import_object_relation(
        &self,
        first: ObjectId,
        second: ObjectId,
    ) -> Result<StorageObjectRelation, StorageError> {
        let (from, to) = ordered_ids(first, second);
        self.state
            .read()
            .await
            .object_relations
            .values()
            .find(|relation| relation.from_object_id() == from && relation.to_object_id() == to)
            .cloned()
            .ok_or_else(|| StorageError::not_found("Import object relation was not found"))
    }

    async fn apply_import_principal(
        &self,
        input: StorageImportPrincipal,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        enum ResolvedSubtype {
            Human {
                password: Option<String>,
                proper_name: Option<String>,
                email: Option<String>,
                anonymized_at: Option<DateTime<Utc>>,
            },
            ServiceAccount {
                description: String,
                owner_group_id: GroupId,
                created_by: Option<PrincipalId>,
                disabled_at: Option<DateTime<Utc>>,
            },
        }

        let parts = input.into_parts();
        let scope_id = self
            .import_identity_scope_id(
                parts.identity_scope_ref.as_deref(),
                parts.identity_scope_key.as_ref(),
                references,
            )
            .await?;
        let subtype = match parts.subtype {
            StorageImportPrincipalSubtype::Human {
                password,
                password_hash,
                proper_name,
                email,
                anonymized_at,
            } => ResolvedSubtype::Human {
                password: password_hash.or(password),
                proper_name,
                email,
                anonymized_at,
            },
            StorageImportPrincipalSubtype::ServiceAccount {
                description,
                owner_group_ref,
                owner_group_key,
                created_by_ref,
                created_by_key,
                disabled_at,
            } => ResolvedSubtype::ServiceAccount {
                description,
                owner_group_id: self
                    .import_group_id(
                        owner_group_ref.as_deref(),
                        owner_group_key.as_ref(),
                        references,
                    )
                    .await?,
                created_by: match (created_by_ref.as_deref(), created_by_key.as_ref()) {
                    (None, None) => None,
                    (reference, key) => {
                        Some(self.import_principal_id(reference, key, references).await?)
                    }
                },
                disabled_at,
            },
        };
        let mut state = self.state.write().await;
        let existing = state
            .principals
            .values()
            .find(|principal| {
                principal.identity_scope_id() == scope_id && principal.name() == parts.name
            })
            .cloned();
        if let Some(current) = &existing {
            assert_import_revision(parts.condition, current.revision())?;
            if !overwrite {
                return Err(StorageError::conflict("Import principal already exists"));
            }
        } else {
            assert_import_create_condition(parts.condition)?;
        }
        let id = existing.as_ref().map_or_else(
            || {
                let id = PrincipalId::new(state.next_principal_id)
                    .expect("memory import principal id is positive");
                state.next_principal_id += 1;
                id
            },
            StoragePrincipal::id,
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
        let kind = match subtype {
            ResolvedSubtype::Human { .. } => PrincipalKind::Human,
            ResolvedSubtype::ServiceAccount { .. } => PrincipalKind::ServiceAccount,
        };
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).expect("principal id is a valid resource id"),
            created_at,
            updated_at,
            revision,
        )
        .map_err(invalid_contract_value)?;
        let principal = StoragePrincipal::builder(metadata, kind, &parts.name, scope_id)
            .provider_managed(parts.provider_managed)
            .settings(parts.settings)
            .external_subject(parts.external_subject.clone())
            .last_sync_attempted_at(parts.last_sync_attempted_at)
            .last_sync_success_at(parts.last_sync_success_at)
            .try_build()
            .map_err(invalid_contract_value)?;
        match subtype {
            ResolvedSubtype::Human {
                password,
                proper_name,
                email,
                anonymized_at,
            } => {
                let user_id = UserId::new(id.id()).expect("principal id is a valid user id");
                if (password.is_some() || anonymized_at.is_some()) && existing.is_some() {
                    state.invalidate_principal_sessions(id.id());
                    for token in state.tokens.values_mut() {
                        if token.principal_id == id && token.revoked_at.is_none() {
                            token.revoked_at = Some(now);
                        }
                    }
                }
                let password = password.or_else(|| {
                    state.users.get(&id.id()).and_then(|record| {
                        record
                            .user
                            .clone()
                            .into_parts()
                            .password_hash()
                            .map(str::to_owned)
                    })
                });
                let user = StorageUser::try_new(
                    user_id,
                    password,
                    proper_name,
                    email,
                    created_at,
                    updated_at,
                    anonymized_at,
                )
                .map_err(invalid_contract_value)?;
                state.users.insert(
                    id.id(),
                    MemoryUserRecord {
                        user,
                        identity_scope_id: scope_id,
                        name: parts.name.clone(),
                        provider_managed: parts.provider_managed,
                        external_subject: parts.external_subject,
                        last_sync_attempted_at: parts.last_sync_attempted_at,
                        last_sync_success_at: parts.last_sync_success_at,
                    },
                );
                state.service_accounts.remove(&id.id());
            }
            ResolvedSubtype::ServiceAccount {
                description,
                owner_group_id,
                created_by,
                disabled_at,
            } => {
                let account = StorageServiceAccount::try_new(
                    ServiceAccountId::new(id.id())
                        .expect("principal id is a valid service-account id"),
                    description,
                    owner_group_id,
                    created_by,
                    disabled_at,
                    created_at,
                    updated_at,
                )
                .map_err(invalid_contract_value)?;
                state.store_service_account(id.id(), account);
                state.users.remove(&id.id());
            }
        }
        state.store_principal(id.id(), principal);
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Principal(id));
        }
        Ok(Some(revision))
    }
}

fn memory_import_event_context() -> EventContext {
    MEMORY_EXECUTION_SCOPE
        .try_with(|scope| scope.mutation_provenance_override().cloned().flatten())
        .ok()
        .flatten()
        .map(EventContext::from_mutation)
        .unwrap_or_else(EventContext::system)
}
