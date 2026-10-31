use super::*;

impl MemoryStorage {
    pub(super) async fn apply_create_class_relation(
        &self,
        input: StorageImportClassRelation,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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
        let prepared = self
            .prepare_class_relation(
                StorageClassRelationCreate::builder(from_class_id, to_class_id)
                    .labels(parts.forward_label, parts.reverse_label)
                    .template_aliases(parts.forward_template_alias, parts.reverse_template_alias)
                    .relation_limits(parts.from_max_relations, parts.to_max_relations)
                    .build(),
            )
            .await?;
        let created = self
            .create_class_relation(&prepared, &memory_import_event_context())
            .await?
            .into_value();
        Ok(Some(created.relation().metadata().revision()))
    }

    pub(super) async fn apply_check_class_relation_condition(
        &self,
        input: StorageImportClassRelation,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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

    pub(super) async fn apply_update_class_relation_timestamps(
        &self,
        input: StorageImportClassRelation,
        timestamps: StorageImportTimestamps,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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
        let updated = current
            .try_with_metadata(metadata)
            .map_err(invalid_contract_value)?;
        self.state
            .write()
            .await?
            .class_relations
            .insert(ClassRelationId::from(updated.metadata().id()).id(), updated);
        Ok(Some(revision))
    }

    pub(super) async fn apply_create_object_relation(
        &self,
        input: StorageImportObjectRelation,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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
        let state = self.state.read().await?;
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
            .create_object_relation(&prepared, &memory_import_event_context())
            .await?
            .into_value();
        Ok(Some(created.relation().metadata().revision()))
    }

    pub(super) async fn apply_check_object_relation_condition(
        &self,
        input: StorageImportObjectRelation,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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

    pub(super) async fn apply_update_object_relation_timestamps(
        &self,
        input: StorageImportObjectRelation,
        timestamps: StorageImportTimestamps,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
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
        self.state.write().await?.object_relations.insert(
            ObjectRelationId::from(updated.metadata().id()).id(),
            updated,
        );
        Ok(Some(revision))
    }
}
