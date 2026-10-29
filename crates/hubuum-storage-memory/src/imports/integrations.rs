use super::*;

impl MemoryStorage {
    pub(super) async fn apply_upsert_export_template(
        &self,
        input: StorageImportExportTemplate,
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
        let class_id = match (parts.class_ref.as_deref(), parts.class_key.as_ref()) {
            (None, None) => None,
            (reference, key) => Some(self.import_class_id(reference, key, references).await?),
        };
        let mut state = self.state.write().await?;
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
                ResourceId::new(id.id()).expect("export-template id is a valid resource id"),
                created_at,
                updated_at,
                revision,
            )
            .map_err(invalid_contract_value)?,
            collection_id,
            parts.name,
            definition,
        );
        state.store_export_template(id.id(), template);
        Ok(Some(revision))
    }

    pub(super) async fn apply_upsert_remote_target(
        &self,
        input: StorageImportRemoteTarget,
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
        let class_id = match (parts.class_ref.as_deref(), parts.class_key.as_ref()) {
            (None, None) => None,
            (reference, key) => Some(self.import_class_id(reference, key, references).await?),
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
        let mut state = self.state.write().await?;
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
        state.store_remote_target(id.id(), target);
        Ok(Some(revision))
    }

    pub(super) async fn apply_upsert_event_sink(
        &self,
        input: StorageImportEventSink,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        let parts = input.into_parts();
        let mut state = self.state.write().await?;
        let existing = state
            .event_sinks
            .values()
            .find(|sink| sink.name() == parts.name)
            .cloned();
        if let Some(current) = &existing {
            if current.collection_id().is_some() {
                return Err(StorageError::invalid_input(
                    "Global integration imports cannot replace collection-owned sinks",
                ));
            }
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
        let sink =
            StorageEventSink::builder(id, parts.name, parts.kind, created_at, updated_at, revision)
                .configuration(parts.config)
                .delivery_policy(parts.delivery_policy)
                .secret_ref(parts.secret_ref)
                .enabled(parts.enabled)
                .try_build()
                .map_err(invalid_contract_value)?;
        state.store_event_sink(id.id(), sink);
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::EventSink(id));
        }
        Ok(Some(revision))
    }

    pub(super) async fn apply_upsert_event_subscription(
        &self,
        input: StorageImportEventSubscription,
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
                Action::parse(value).map_err(|error| StorageError::invalid_input(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let filter = serde_json::from_value(parts.filter)
            .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        let mut state = self.state.write().await?;
        state.store_sink_grant(sink_id, collection_id);
        let existing = state
            .event_subscriptions
            .values()
            .find(|subscription| {
                subscription.scope().collection_id() == Some(collection_id)
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
        state.store_event_subscription(id.id(), subscription);
        Ok(Some(revision))
    }
}
