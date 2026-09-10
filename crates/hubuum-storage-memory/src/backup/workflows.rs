use super::*;

mod history;
pub(super) use history::{capture_history, restore_history};

pub(super) fn capture(
    state: &MemoryState,
    sections: &mut StorageBackupStateSections,
) -> Result<(), StorageError> {
    sections.insert(StorageBackupStateSection::EventSinks, state.event_sinks.values().map(|v| row(json!({"id": v.id().id(), "name": v.name(), "kind": v.kind(), "config": v.configuration(), "secret_ref": v.secret_ref(), "enabled": v.enabled(), "created_at": v.created_at(), "updated_at": v.updated_at(), "revision": v.revision().get()}))).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::EventSubscriptions, state.event_subscriptions.values().map(|v| row(json!({"id": v.id().id(), "collection_id": v.collection_id().id(), "sink_id": v.sink_id().id(), "name": v.name(), "description": v.description(), "entity_types": v.entity_types(), "actions": v.actions(), "filter": v.filter(), "routing": v.routing(), "enabled": v.enabled(), "created_at": v.created_at(), "updated_at": v.updated_at(), "revision": v.revision().get()}))).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::ComputedFieldDefinitions, state.computed_fields.values().map(|v| {
        let (visibility, owner) = match v.visibility() { StorageComputedFieldVisibility::Shared => ("shared", None), StorageComputedFieldVisibility::Personal { owner_id } => ("personal", Some(owner_id.id())) };
        let m = v.metadata();
        row(json!({"id": m.id().id(), "class_id": v.class_id().id(), "visibility": visibility, "owner_principal_id": owner,
            "key": v.key(), "label": v.label(), "description": v.description(), "operation": v.operation(), "result_type": v.result_type(),
            "enabled": v.enabled(), "revision": m.revision().get(), "semantics_version": v.semantics_version(),
            "created_by": v.created_by().map(PrincipalId::id), "updated_by": v.updated_by().map(PrincipalId::id), "created_at": m.created_at(), "updated_at": m.updated_at()}))
    }).collect::<Result<_, _>>()?);
    Ok(())
}

pub(super) fn restore(
    sections: &StorageBackupStateSections,
    state: &mut MemoryState,
) -> Result<(), StorageError> {
    for row in &sections[&StorageBackupStateSection::EventSinks] {
        let r = Row(row);
        let value = StorageEventSink::builder(
            EventSinkId::new(r.integer("id")?).map_err(|_| invalid("id"))?,
            r.text("name")?,
            r.text("kind")?,
            r.time("created_at")?,
            r.time("updated_at")?,
            r.revision()?,
        )
        .configuration(r.value("config")?.clone())
        .secret_ref(r.optional_text("secret_ref")?)
        .enabled(r.boolean("enabled")?)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_sinks.insert(value.id().id(), value);
    }
    for row in &sections[&StorageBackupStateSection::EventSubscriptions] {
        let r = Row(row);
        let value = StorageEventSubscription::builder(
            EventSubscriptionId::new(r.integer("id")?).map_err(|_| invalid("id"))?,
            CollectionId::new(r.integer("collection_id")?).map_err(|_| invalid("collection_id"))?,
            EventSinkId::new(r.integer("sink_id")?).map_err(|_| invalid("sink_id"))?,
            r.text("name")?,
            r.time("created_at")?,
            r.time("updated_at")?,
            r.revision()?,
        )
        .description(r.text("description")?)
        .entity_types(
            serde_json::from_value(r.value("entity_types")?.clone())
                .map_err(|_| invalid("entity_types"))?,
        )
        .actions(
            serde_json::from_value(r.value("actions")?.clone()).map_err(|_| invalid("actions"))?,
        )
        .filter(serde_json::from_value(r.value("filter")?.clone()).map_err(|_| invalid("filter"))?)
        .routing(r.value("routing")?.clone())
        .enabled(r.boolean("enabled")?)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_subscriptions.insert(value.id().id(), value);
    }
    for row in &sections[&StorageBackupStateSection::ComputedFieldDefinitions] {
        let r = Row(row);
        let input = Definition::try_from_parts(
            FieldKey::new(r.text("key")?).map_err(|_| invalid("key"))?,
            r.text("label")?,
            r.text("description")?,
            serde_json::from_value(r.value("operation")?.clone())
                .map_err(|_| invalid("operation"))?,
            serde_json::from_value(r.value("result_type")?.clone())
                .map_err(|_| invalid("result_type"))?,
            r.boolean("enabled")?,
            i16::try_from(r.integer("semantics_version")?)
                .map_err(|_| invalid("semantics_version"))?,
        )
        .map_err(|_| invalid("computed definition"))?;
        let visibility = match r.text("visibility")? {
            "shared" => StorageComputedFieldVisibility::Shared,
            "personal" => StorageComputedFieldVisibility::Personal {
                owner_id: PrincipalId::new(r.integer("owner_principal_id")?)
                    .map_err(|_| invalid("owner_principal_id"))?,
            },
            _ => return Err(invalid("visibility")),
        };
        let provenance = StorageComputedFieldProvenance::new(
            r.optional_integer("created_by")?
                .map(PrincipalId::new)
                .transpose()
                .map_err(|_| invalid("created_by"))?,
            r.optional_integer("updated_by")?
                .map(PrincipalId::new)
                .transpose()
                .map_err(|_| invalid("updated_by"))?,
        );
        let value = StorageComputedFieldDefinition::new(
            r.metadata()?,
            ClassId::new(r.integer("class_id")?).map_err(|_| invalid("class_id"))?,
            visibility,
            StorageComputedFieldDefinitionContent::new(StorageComputedFieldDefinitionInput::new(
                input,
            )),
            provenance,
        );
        state
            .computed_fields
            .insert(value.metadata().id().id(), value);
    }
    state.next_event_sink_id = next_id(&state.event_sinks)?;
    state.next_event_subscription_id = next_id(&state.event_subscriptions)?;
    state.next_computed_field_id = next_id(&state.computed_fields)?;
    Ok(())
}
