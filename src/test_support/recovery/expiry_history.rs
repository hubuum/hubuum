//! Productive mutations for the signed history-expiry restore contract.
use super::*;
use hubuum_domain::RelationLabel;

impl RestoreContractFixture {
    pub async fn close_class_relation_versions(
        &self,
        class: &StorageClass,
    ) -> Result<(), ApiError> {
        let (_, mut relation) = self.create_expiry_class_relation(class).await?;
        for index in 1..=3 {
            let label = RelationLabel::new(format!("restore label {index}")).unwrap();
            relation = self
                .storage
                .class_relation_store()
                .update_class_relation_labels(
                    &relation,
                    StorageClassRelationLabelsUpdate::new(Some(Some(label)), None),
                    &EventContext::system(),
                )
                .await?
                .into_value();
        }
        Ok(())
    }

    async fn create_expiry_class_relation(
        &self,
        class: &StorageClass,
    ) -> Result<(StorageClass, StorageResolvedClassRelation), ApiError> {
        let other = self.create_class("expiry_relation_peer").await?;
        let relations = self.storage.class_relation_store();
        let prepared = relations
            .prepare_class_relation(
                StorageClassRelationCreate::builder(class.id(), other.id()).build(),
            )
            .await?;
        let relation = relations
            .create_class_relation(&prepared, &EventContext::system())
            .await?
            .into_value();
        Ok((other, relation))
    }

    pub async fn close_object_relation_versions(
        &self,
        class: &StorageClass,
    ) -> Result<(), ApiError> {
        let (peer, relation) = self.create_expiry_class_relation(class).await?;
        let left = self.create_object(class.id(), "expiry_left").await?;
        let right = self.create_object(peer.id(), "expiry_right").await?;
        let relations = self.storage.object_relation_store();
        for index in 0..4 {
            let prepared = relations
                .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                    StorageObjectRelationCreate::new(
                        left.id(),
                        right.id(),
                        relation.relation().metadata().id().into(),
                    ),
                ))
                .await?;
            let edge = relations
                .create_object_relation(&prepared, &EventContext::system())
                .await?
                .into_value();
            // Object edges are immutable. Keep the last edge live across both restores.
            if index < 3 {
                relations
                    .delete_object_relation(&edge, &EventContext::system())
                    .await?
                    .into_value();
            }
        }
        Ok(())
    }

    pub async fn close_export_template_versions(&self) -> Result<(), ApiError> {
        for index in 1..=3 {
            self.storage
                .replace_export_template(StorageExportTemplateReplace::new(
                    self.template.metadata().id().into(),
                    self.collection_id,
                    self.template.name(),
                    StorageExportTemplateDefinition::new(
                        format!("restore revision {index}"),
                        "text/plain",
                        "{}",
                        "fragment",
                    ),
                    EventContext::system(),
                ))
                .await?
                .into_value();
        }
        Ok(())
    }

    pub async fn close_remote_target_versions(&self) -> Result<(), ApiError> {
        let mut target = self.remote.clone();
        for index in 1..=3 {
            target = self
                .storage
                .update_remote_target(StorageRemoteTargetUpdate::new(
                    target.metadata().id().into(),
                    target.metadata().revision(),
                    StorageRemoteTargetPatch::new()
                        .with_description(Some(format!("restore revision {index}"))),
                    EventContext::system(),
                ))
                .await?
                .into_value();
        }
        Ok(())
    }

    pub async fn close_computed_definition_versions(
        &self,
        class: &StorageClass,
    ) -> Result<(), ApiError> {
        for index in 0..4 {
            let definition = serde_json::from_value(json!({
                "key": format!("restore_{index}"),
                "label": "Restore field",
                "description": "",
                "operation": {"type": "first_non_null", "paths": ["/source"]},
                "result_type": "string",
                "enabled": true
            }))
            .unwrap();
            self.storage
                .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
                    class.id(),
                    class.collection_id(),
                    self.actor_id,
                    StorageComputedFieldDefinitionInput::new(definition),
                    EventContext::system(),
                ))
                .await?
                .into_value();
        }
        Ok(())
    }

    pub async fn create_expirable_schema_events(&self) -> Result<(), ApiError> {
        let class = self.create_class("expiry_retired_schema").await?;
        let context = EventContext::system();
        let schema = StorageValidatedSchemaPolicy::try_new(
            StorageClassSchemaPolicy::try_from_parts(Some(json!({"type":"object"})), true).unwrap(),
        )
        .unwrap();
        let staged = self
            .storage
            .stage_schema_revision(StorageSchemaStage::new(
                class.collection_id(),
                class.id(),
                schema,
                context.clone(),
            ))
            .await?
            .into_value();
        self.storage
            .abandon_schema_revision(staged.reference(), class.collection_id(), &context)
            .await?
            .into_value();
        // Any retained revision of this class protects its events.
        let classes = self.storage.class_store();
        let resolved = classes
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await?;
        classes
            .delete_class(&resolved, &context)
            .await?
            .into_value();
        Ok(())
    }
}
