use super::*;

impl MemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::new())),
        }
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
            .find(|sink| sink.name() == name)
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
                state.identity_scopes.insert(id.id(), scope);
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::IdentityScope(id));
                }
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertGroup { input, overwrite } => {
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
                    .find(|group| {
                        group.identity_scope_id() == scope_id && group.name() == parts.name
                    })
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
                        let id = GroupId::new(state.next_group_id)
                            .expect("memory import group id is positive");
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
                state.groups.insert(id.id(), group);
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Group(id));
                }
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertPrincipal { input, overwrite } => {
                self.apply_import_principal(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertGroupMembership { input, overwrite } => {
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
                state
                    .memberships
                    .insert((principal_id.id(), group_id.id()), membership);
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
            StorageImportOperation::CreateCollection(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let parent_collection_id = if let Some(reference) = parts.parent_collection_ref {
                    match references.get(&reference) {
                        Some(MemoryImportReference::Collection(id)) => Some(*id),
                        _ => {
                            return Err(StorageError::not_found(format!(
                                "Import collection reference '{reference}' was not found"
                            )));
                        }
                    }
                } else if let Some(key) = parts.parent_collection_key {
                    self.get_import_collection_by_key(&key)
                        .await?
                        .map(|collection| collection.id())
                        .ok_or_else(|| {
                            StorageError::not_found("Import parent collection was not found")
                        })?
                        .into()
                } else {
                    Some(CollectionId::new(ROOT_COLLECTION_ID).expect("root id is valid"))
                };
                let owner_group_id = GroupId::new(1).expect("seeded admin group id is valid");
                let created = self
                    .create_collection(
                        StorageCollectionCreate::new(
                            parts.name,
                            parts.description,
                            owner_group_id,
                            parent_collection_id,
                        ),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Collection(created.id()));
                }
                Ok(Some(created.revision()))
            }
            StorageImportOperation::CreateClass(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let collection_id = if let Some(reference) = parts.collection_ref {
                    match references.get(&reference) {
                        Some(MemoryImportReference::Collection(id)) => *id,
                        _ => {
                            return Err(StorageError::not_found(format!(
                                "Import collection reference '{reference}' was not found"
                            )));
                        }
                    }
                } else if let Some(key) = parts.collection_key {
                    self.get_import_collection_by_key(&key)
                        .await?
                        .map(|collection| collection.id())
                        .ok_or_else(|| StorageError::not_found("Import collection was not found"))?
                } else {
                    return Err(StorageError::invalid_input(
                        "Import class requires a collection selector",
                    ));
                };
                let created = self
                    .create_class(
                        StorageClassCreate::builder(parts.name, collection_id, parts.description)
                            .json_schema(parts.json_schema)
                            .validate_schema(parts.validate_schema)
                            .build(),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Class(created.id()));
                }
                Ok(Some(created.revision()))
            }
            StorageImportOperation::UpdateCollection {
                collection_id,
                input,
            } => {
                let parts = input.into_parts();
                let current = self.get_collection(collection_id).await?;
                assert_import_revision(parts.condition, current.revision())?;
                let updated = self
                    .update_collection(
                        collection_id,
                        StorageCollectionUpdate::new(Some(parts.name), Some(parts.description)),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Collection(updated.id()));
                }
                Ok(Some(updated.revision()))
            }
            StorageImportOperation::UpdateClass { class_id, input } => {
                let parts = input.into_parts();
                let target = self
                    .resolve_class(StorageClassSelector::Id(class_id))
                    .await?;
                assert_import_revision(parts.condition, target.class().revision())?;
                let updated = self
                    .update_class(
                        &target,
                        StorageClassUpdate::builder()
                            .name(Some(parts.name))
                            .json_schema(parts.json_schema)
                            .validate_schema(Some(parts.validate_schema))
                            .description(Some(parts.description))
                            .build(),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Class(updated.id()));
                }
                Ok(Some(updated.revision()))
            }
            StorageImportOperation::CreateObject(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let class_id = self
                    .import_class_id(
                        parts.class_ref.as_deref(),
                        parts.class_key.as_ref(),
                        references,
                    )
                    .await?;
                let class = self
                    .resolve_class(StorageClassSelector::Id(class_id))
                    .await?;
                let created = self
                    .create_object(
                        &class,
                        StorageObjectCreate::new(
                            parts.name,
                            class.class().collection_id(),
                            class_id,
                            parts.data,
                            parts.description,
                        ),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Object(created.id()));
                }
                Ok(Some(created.revision()))
            }
            StorageImportOperation::UpdateObject { object_id, input } => {
                let parts = input.into_parts();
                let current = self
                    .state
                    .read()
                    .await
                    .objects
                    .get(&object_id.id())
                    .cloned()
                    .ok_or_else(|| StorageError::not_found("Import object was not found"))?;
                assert_import_revision(parts.condition, current.revision())?;
                let target = self
                    .resolve_object(StorageObjectSelector::Ids {
                        class_id: current.class_id(),
                        object_id,
                    })
                    .await?;
                let updated = self
                    .update_object(
                        &target,
                        StorageObjectUpdate::builder()
                            .name(Some(parts.name))
                            .data(Some(parts.data))
                            .description(Some(parts.description))
                            .build(),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Object(updated.id()));
                }
                Ok(Some(updated.revision()))
            }
            StorageImportOperation::CreateClassRelation(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let from_class_id = self
                    .import_class_id(
                        parts.from_class_ref.as_deref(),
                        parts.from_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_class_id = self
                    .import_class_id(
                        parts.to_class_ref.as_deref(),
                        parts.to_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let (from_class_id, to_class_id) = ordered_ids(from_class_id, to_class_id);
                let prepared = self
                    .prepare_class_relation(
                        StorageClassRelationCreate::builder(from_class_id, to_class_id)
                            .template_aliases(
                                parts.forward_template_alias,
                                parts.reverse_template_alias,
                            )
                            .relation_limits(parts.from_max_relations, parts.to_max_relations)
                            .build(),
                    )
                    .await?;
                let created = self
                    .create_class_relation(&prepared, &EventContext::system())
                    .await?
                    .into_value();
                Ok(Some(created.relation().metadata().revision()))
            }
            StorageImportOperation::CheckClassRelationCondition(input) => {
                let parts = input.into_parts();
                let from_class_id = self
                    .import_class_id(
                        parts.from_class_ref.as_deref(),
                        parts.from_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_class_id = self
                    .import_class_id(
                        parts.to_class_ref.as_deref(),
                        parts.to_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let relation = self
                    .import_class_relation(from_class_id, to_class_id)
                    .await?;
                assert_import_revision(parts.condition, relation.metadata().revision())?;
                Ok(Some(relation.metadata().revision()))
            }
            StorageImportOperation::UpdateClassRelationTimestamps { input, timestamps } => {
                let parts = input.into_parts();
                let from_class_id = self
                    .import_class_id(
                        parts.from_class_ref.as_deref(),
                        parts.from_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_class_id = self
                    .import_class_id(
                        parts.to_class_ref.as_deref(),
                        parts.to_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let current = self
                    .import_class_relation(from_class_id, to_class_id)
                    .await?;
                assert_import_revision(parts.condition, current.metadata().revision())?;
                let revision = current
                    .metadata()
                    .revision()
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let (created_at, updated_at) = timestamps.into_parts();
                let metadata = StorageRecordMetadata::try_new(
                    current.metadata().id(),
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                let updated = StorageClassRelation::try_new(
                    metadata,
                    current.from_class_id(),
                    current.to_class_id(),
                )
                .and_then(|relation| {
                    relation.try_with_template_aliases(
                        current.forward_template_alias().map(ToOwned::to_owned),
                        current.reverse_template_alias().map(ToOwned::to_owned),
                    )
                })
                .and_then(|relation| {
                    relation.try_with_relation_limits(
                        current.from_max_relations(),
                        current.to_max_relations(),
                    )
                })
                .map_err(invalid_contract_value)?;
                self.state
                    .write()
                    .await
                    .class_relations
                    .insert(ClassRelationId::from(updated.metadata().id()).id(), updated);
                Ok(Some(revision))
            }
            StorageImportOperation::CreateObjectRelation(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let from_object_id = self
                    .import_object_id(
                        parts.from_object_ref.as_deref(),
                        parts.from_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_object_id = self
                    .import_object_id(
                        parts.to_object_ref.as_deref(),
                        parts.to_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let (from_object_id, to_object_id) = ordered_ids(from_object_id, to_object_id);
                let state = self.state.read().await;
                let from = state
                    .objects
                    .get(&from_object_id.id())
                    .ok_or_else(|| StorageError::not_found("Import object was not found"))?;
                let to = state
                    .objects
                    .get(&to_object_id.id())
                    .ok_or_else(|| StorageError::not_found("Import object was not found"))?;
                let from_class_id = from.class_id();
                let to_class_id = to.class_id();
                drop(state);
                let class_relation = self
                    .import_class_relation(from_class_id, to_class_id)
                    .await?;
                let prepared = self
                    .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                        StorageObjectRelationCreate::new(
                            from_object_id,
                            to_object_id,
                            ClassRelationId::from(class_relation.metadata().id()),
                        ),
                    ))
                    .await?;
                let created = self
                    .create_object_relation(&prepared, &EventContext::system())
                    .await?
                    .into_value();
                Ok(Some(created.relation().metadata().revision()))
            }
            StorageImportOperation::CheckObjectRelationCondition(input) => {
                let parts = input.into_parts();
                let from_object_id = self
                    .import_object_id(
                        parts.from_object_ref.as_deref(),
                        parts.from_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_object_id = self
                    .import_object_id(
                        parts.to_object_ref.as_deref(),
                        parts.to_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let relation = self
                    .import_object_relation(from_object_id, to_object_id)
                    .await?;
                assert_import_revision(parts.condition, relation.metadata().revision())?;
                Ok(Some(relation.metadata().revision()))
            }
            StorageImportOperation::UpdateObjectRelationTimestamps { input, timestamps } => {
                let parts = input.into_parts();
                let from_object_id = self
                    .import_object_id(
                        parts.from_object_ref.as_deref(),
                        parts.from_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_object_id = self
                    .import_object_id(
                        parts.to_object_ref.as_deref(),
                        parts.to_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let current = self
                    .import_object_relation(from_object_id, to_object_id)
                    .await?;
                assert_import_revision(parts.condition, current.metadata().revision())?;
                let revision = current
                    .metadata()
                    .revision()
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let (created_at, updated_at) = timestamps.into_parts();
                let metadata = StorageRecordMetadata::try_new(
                    current.metadata().id(),
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                let updated = StorageObjectRelation::try_new(
                    metadata,
                    current.from_object_id(),
                    current.to_object_id(),
                    current.class_relation_id(),
                )
                .map_err(invalid_contract_value)?;
                self.state.write().await.object_relations.insert(
                    ObjectRelationId::from(updated.metadata().id()).id(),
                    updated,
                );
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertComputedField { input, overwrite } => {
                let parts = input.into_parts();
                let class_id = self
                    .import_class_id(
                        parts.class_ref.as_deref(),
                        parts.class_key.as_ref(),
                        references,
                    )
                    .await?;
                let visibility = match parts.visibility {
                    StorageImportComputedFieldVisibility::Shared => {
                        StorageComputedFieldVisibility::Shared
                    }
                    StorageImportComputedFieldVisibility::Personal => {
                        StorageComputedFieldVisibility::Personal {
                            owner_id: self
                                .import_principal_id(
                                    parts.owner_ref.as_deref(),
                                    parts.owner_key.as_ref(),
                                    references,
                                )
                                .await?,
                        }
                    }
                };
                let mut state = self.state.write().await;
                let existing = state
                    .computed_fields
                    .values()
                    .find(|definition| {
                        definition.class_id() == class_id
                            && definition.visibility() == visibility
                            && definition.key() == parts.key
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.metadata().revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import computed field already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = ComputedFieldDefinitionId::new(state.next_computed_field_id)
                            .expect("memory import computed-field id is positive");
                        state.next_computed_field_id += 1;
                        id
                    },
                    |value| ComputedFieldDefinitionId::from(value.metadata().id()),
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.metadata().created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.metadata().revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let metadata = StorageRecordMetadata::try_new(
                    ResourceId::new(id.id()).expect("computed-field id is a valid resource id"),
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                let actor_id = match visibility {
                    StorageComputedFieldVisibility::Shared => {
                        PrincipalId::new(1).expect("seeded administrator principal id is valid")
                    }
                    StorageComputedFieldVisibility::Personal { owner_id } => owner_id,
                };
                let definition = StorageComputedFieldDefinition::new(
                    metadata,
                    class_id,
                    visibility,
                    StorageComputedFieldDefinitionContent::new(
                        StorageComputedFieldDefinitionInput::new(
                            parts.key,
                            parts.label,
                            parts.operation,
                            parts.result_type,
                        )
                        .with_description(parts.description)
                        .with_enabled(parts.enabled),
                        1,
                    ),
                    StorageComputedFieldProvenance::new(Some(actor_id), Some(actor_id)),
                );
                state.computed_fields.insert(id.id(), definition);
                Ok(Some(revision))
            }
            StorageImportOperation::ApplyCollectionPermissions { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let group_id = self
                    .import_group_id(None, Some(&parts.group_key), references)
                    .await?;
                let collection = self.get_collection(collection_id).await?;
                assert_import_revision(parts.condition, collection.revision())?;
                self.apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                    StorageAuthorizationGrantKey::new(collection_id, group_id),
                    parts.permissions,
                    overwrite || parts.replace_existing,
                    EventContext::system(),
                ))
                .await?
                .into_value();
                Ok(Some(collection.revision()))
            }
            StorageImportOperation::UpsertExportTemplate { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let class_id = match (parts.class_ref.as_deref(), parts.class_key.as_ref()) {
                    (None, None) => None,
                    (reference, key) => {
                        Some(self.import_class_id(reference, key, references).await?)
                    }
                };
                let mut state = self.state.write().await;
                let existing = state
                    .export_templates
                    .values()
                    .find(|template| {
                        let (_, current_collection_id, name, _) = (*template).clone().into_parts();
                        current_collection_id == collection_id && name == parts.name
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.metadata().revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import export template already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = ExportTemplateId::new(state.next_export_template_id)
                            .expect("memory import export-template id is positive");
                        state.next_export_template_id += 1;
                        id
                    },
                    |value| ExportTemplateId::from(value.metadata().id()),
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.metadata().created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.metadata().revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let definition = StorageExportTemplateDefinition::new(
                    parts.description,
                    parts.content_type,
                    parts.template,
                    parts.kind,
                )
                .with_scope(parts.scope_kind, class_id)
                .with_default_query(parts.default_query)
                .with_include(parts.include)
                .with_relation_context(parts.relation_context)
                .with_default_missing_data_policy(parts.default_missing_data_policy)
                .with_default_limits(parts.default_limits);
                let template = StorageExportTemplate::new(
                    StorageRecordMetadata::try_new(
                        ResourceId::new(id.id())
                            .expect("export-template id is a valid resource id"),
                        created_at,
                        updated_at,
                        revision,
                    )
                    .map_err(invalid_contract_value)?,
                    collection_id,
                    parts.name,
                    definition,
                );
                state.export_templates.insert(id.id(), template);
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertRemoteTarget { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let class_id = match (parts.class_ref.as_deref(), parts.class_key.as_ref()) {
                    (None, None) => None,
                    (reference, key) => {
                        Some(self.import_class_id(reference, key, references).await?)
                    }
                };
                let transport = StorageRemoteTargetTransport::try_new(
                    parts.method,
                    parts.url_template,
                    parts.headers_template,
                    parts.body_template,
                    parts.auth_config,
                    parts.timeout_ms,
                )
                .map_err(invalid_contract_value)?;
                let policy = StorageRemoteTargetPolicy::try_new(
                    class_id,
                    parts.allowed_subject_types,
                    parts.enabled,
                )
                .map_err(invalid_contract_value)?;
                let mut state = self.state.write().await;
                let existing = state
                    .remote_targets
                    .values()
                    .find(|target| {
                        let (_, current_collection_id, name, _) = (*target).clone().into_parts();
                        current_collection_id == collection_id && name == parts.name
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.metadata().revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import remote target already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = RemoteTargetId::new(state.next_remote_target_id)
                            .expect("memory import remote-target id is positive");
                        state.next_remote_target_id += 1;
                        id
                    },
                    |value| RemoteTargetId::from(value.metadata().id()),
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.metadata().created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.metadata().revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let target = StorageRemoteTarget::new(
                    StorageRecordMetadata::try_new(
                        ResourceId::new(id.id()).expect("remote-target id is a valid resource id"),
                        created_at,
                        updated_at,
                        revision,
                    )
                    .map_err(invalid_contract_value)?,
                    collection_id,
                    parts.name,
                    StorageRemoteTargetDefinition::new(parts.description, transport, policy),
                );
                state.remote_targets.insert(id.id(), target);
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertEventSink { input, overwrite } => {
                let parts = input.into_parts();
                let mut state = self.state.write().await;
                let existing = state
                    .event_sinks
                    .values()
                    .find(|sink| sink.name() == parts.name)
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict("Import event sink already exists"));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = EventSinkId::new(state.next_event_sink_id)
                            .expect("memory import event-sink id is positive");
                        state.next_event_sink_id += 1;
                        id
                    },
                    StorageEventSink::id,
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
                let sink = StorageEventSink::builder(
                    id, parts.name, parts.kind, created_at, updated_at, revision,
                )
                .configuration(parts.config)
                .secret_ref(parts.secret_ref)
                .enabled(parts.enabled)
                .try_build()
                .map_err(invalid_contract_value)?;
                state.event_sinks.insert(id.id(), sink);
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::EventSink(id));
                }
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertEventSubscription { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let sink_id = self
                    .import_event_sink_id(
                        parts.sink_ref.as_deref(),
                        parts.sink_key.as_ref(),
                        references,
                    )
                    .await?;
                let entity_types = parts
                    .entity_types
                    .iter()
                    .map(|value| {
                        EntityType::parse(value)
                            .map_err(|error| StorageError::invalid_input(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let actions = parts
                    .actions
                    .iter()
                    .map(|value| {
                        Action::parse(value)
                            .map_err(|error| StorageError::invalid_input(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let filter = serde_json::from_value(parts.filter)
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
                let mut state = self.state.write().await;
                let existing = state
                    .event_subscriptions
                    .values()
                    .find(|subscription| {
                        subscription.collection_id() == collection_id
                            && subscription.name() == parts.name
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import event subscription already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = EventSubscriptionId::new(state.next_event_subscription_id)
                            .expect("memory import event-subscription id is positive");
                        state.next_event_subscription_id += 1;
                        id
                    },
                    StorageEventSubscription::id,
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
                let subscription = StorageEventSubscription::builder(
                    id,
                    collection_id,
                    sink_id,
                    parts.name,
                    created_at,
                    updated_at,
                    revision,
                )
                .description(parts.description)
                .entity_types(entity_types)
                .actions(actions)
                .filter(filter)
                .routing(parts.routing)
                .enabled(parts.enabled)
                .try_build()
                .map_err(invalid_contract_value)?;
                state.event_subscriptions.insert(id.id(), subscription);
                Ok(Some(revision))
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
                state.service_accounts.insert(id.id(), account);
                state.users.remove(&id.id());
            }
        }
        state.principals.insert(id.id(), principal);
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Principal(id));
        }
        Ok(Some(revision))
    }
}
