use super::*;

impl MemoryStorage {
    pub(super) async fn apply_create_collection(
        &self,
        input: StorageImportCollection,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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
                .ok_or_else(|| StorageError::not_found("Import parent collection was not found"))?
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
                &memory_import_event_context(),
            )
            .await?
            .into_value();
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Collection(created.id()));
        }
        Ok(Some(created.revision()))
    }

    pub(super) async fn apply_create_class(
        &self,
        input: StorageImportClass,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        if parts.schema_activation.is_some() {
            return Err(StorageError::invalid_input(
                "Schema activation requires an existing class",
            ));
        }
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
                    .schema_policy(parts.schema_policy)
                    .build(),
                &memory_import_event_context(),
            )
            .await?
            .into_value();
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Class(created.id()));
        }
        Ok(Some(created.revision()))
    }

    pub(super) async fn apply_update_collection(
        &self,
        collection_id: CollectionId,
        input: StorageImportCollection,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let current = self.get_collection(collection_id).await?;
        assert_import_revision(parts.condition, current.revision())?;
        let updated = self
            .update_collection(
                collection_id,
                StorageCollectionUpdate::new(Some(parts.name), Some(parts.description)),
                &memory_import_event_context(),
            )
            .await?
            .into_value();
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Collection(updated.id()));
        }
        Ok(Some(updated.revision()))
    }

    pub(super) async fn apply_update_class(
        &self,
        class_id: ClassId,
        input: StorageImportClass,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let mut target = self
            .resolve_class(StorageClassSelector::Id(class_id))
            .await?;
        let authorized_collection = self
            .import_collection_id(
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
                references,
            )
            .await?;
        if target.class().collection_id() != authorized_collection {
            return Err(StorageError::not_found(
                "Imported class no longer belongs to the selected collection",
            ));
        }
        assert_import_revision(parts.condition, target.class().revision())?;
        if let Some(intent) = &parts.schema_activation {
            let revision = self
                .list_schema_revisions(
                    StorageSchemaPage::try_new(class_id, intent.revision().get() - 1, 1)
                        .map_err(invalid_contract_value)?,
                )
                .await?
                .into_iter()
                .find(|revision| revision.reference().revision() == intent.revision())
                .ok_or_else(|| StorageError::not_found("Imported schema revision was not found"))?;
            if revision.policy().policy() != &parts.schema_policy {
                return Err(StorageError::invalid_input(
                    "Imported class policy must exactly match the selected staged schema revision",
                ));
            }
            self.activate_schema_revision(intent.for_class(
                class_id,
                target.class().collection_id(),
                memory_import_event_context(),
            ))
            .await?
            .into_value();
            target = self
                .resolve_class(StorageClassSelector::Id(class_id))
                .await?;
        }
        let (json_schema, validate_schema) = parts.schema_policy.into_parts();
        let updated = self
            .update_class(
                &target,
                StorageClassUpdate::builder()
                    .name(Some(parts.name))
                    .json_schema(json_schema)
                    .validate_schema(Some(validate_schema))
                    .description(Some(parts.description))
                    .build(),
                &memory_import_event_context(),
            )
            .await?
            .into_value();
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Class(updated.id()));
        }
        Ok(Some(updated.revision()))
    }

    pub(super) async fn apply_create_object(
        &self,
        input: StorageImportObject,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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
                &memory_import_event_context(),
            )
            .await?
            .into_value();
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Object(created.id()));
        }
        Ok(Some(created.revision()))
    }

    pub(super) async fn apply_update_object(
        &self,
        object_id: ObjectId,
        input: StorageImportObject,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let current = self
            .state
            .read()
            .await
            .objects
            .get(&object_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Import object was not found"))?;
        let selected_class = self
            .import_class_id(
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
                references,
            )
            .await?;
        if current.class_id() != selected_class {
            return Err(StorageError::not_found(
                "Imported object no longer belongs to the selected class",
            ));
        }
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
                &memory_import_event_context(),
            )
            .await?
            .into_value();
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Object(updated.id()));
        }
        Ok(Some(updated.revision()))
    }

    pub(super) async fn apply_upsert_computed_field(
        &self,
        input: StorageImportComputedField,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let definition_key = parts.definition.key().as_str().to_string();
        let class_id = self
            .import_class_id(
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
                references,
            )
            .await?;
        let visibility = match parts.visibility {
            StorageImportComputedFieldVisibility::Shared => StorageComputedFieldVisibility::Shared,
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
                    && definition.key() == definition_key
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
        let evaluator_definition = parts.definition;
        let definition = StorageComputedFieldDefinition::new(
            metadata,
            class_id,
            visibility,
            StorageComputedFieldDefinitionContent::new(StorageComputedFieldDefinitionInput::new(
                evaluator_definition,
            )),
            StorageComputedFieldProvenance::new(Some(actor_id), Some(actor_id)),
        );
        state.store_computed_field(id.id(), definition);
        if visibility == StorageComputedFieldVisibility::Shared {
            state.record_computed_history(class_id)?;
        }
        Ok(Some(revision))
    }

    pub(super) async fn apply_apply_collection_permissions(
        &self,
        input: StorageImportCollectionPermission,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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
            memory_import_event_context(),
        ))
        .await?
        .into_value();
        Ok(Some(collection.revision()))
    }
}
