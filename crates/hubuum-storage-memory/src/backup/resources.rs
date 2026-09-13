use super::*;

pub(super) fn collection(r: &Row<'_>) -> Result<StorageCollection, StorageError> {
    StorageCollection::try_new(
        r.metadata()?,
        r.text("name")?,
        r.text("description")?,
        r.optional_integer("parent_collection_id")?
            .map(CollectionId::new)
            .transpose()
            .map_err(|_| invalid("parent_collection_id"))?,
    )
    .map_err(invalid_contract_value)
}

pub(super) fn class(r: &Row<'_>) -> Result<StorageClass, StorageError> {
    let policy = StorageClassSchemaPolicy::try_from_parts(
        r.optional_value("json_schema").cloned(),
        r.boolean("validate_schema")?,
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageClass::builder(
        r.metadata()?,
        r.text("name")?,
        CollectionId::new(r.integer("collection_id")?).map_err(|_| invalid("collection_id"))?,
        r.text("description")?,
    )
    .schema_policy(policy)
    .build())
}

pub(super) fn object(r: &Row<'_>) -> Result<StorageObject, StorageError> {
    Ok(StorageObject::new(
        r.metadata()?,
        r.text("name")?,
        CollectionId::new(r.integer("collection_id")?).map_err(|_| invalid("collection_id"))?,
        ClassId::new(r.integer("class_id")?).map_err(|_| invalid("class_id"))?,
        r.value("data")?.clone(),
        r.text("description")?,
    ))
}

fn class_relation(r: &Row<'_>) -> Result<StorageClassRelation, StorageError> {
    let forward = r.optional_text("forward_template_alias")?;
    let reverse = r.optional_text("reverse_template_alias")?;
    let from = r.optional_integer("from_max_relations")?;
    let to = r.optional_integer("to_max_relations")?;
    StorageClassRelation::try_new(
        r.metadata()?,
        ClassId::new(r.integer("from_class_id")?).map_err(|_| invalid("from_class_id"))?,
        ClassId::new(r.integer("to_class_id")?).map_err(|_| invalid("to_class_id"))?,
    )
    .and_then(|value| value.try_with_template_aliases(forward, reverse))
    .and_then(|value| value.try_with_relation_limits(from, to))
    .map_err(invalid_contract_value)
}

fn object_relation(r: &Row<'_>) -> Result<StorageObjectRelation, StorageError> {
    StorageObjectRelation::try_new(
        r.metadata()?,
        ObjectId::new(r.integer("from_object_id")?).map_err(|_| invalid("from_object_id"))?,
        ObjectId::new(r.integer("to_object_id")?).map_err(|_| invalid("to_object_id"))?,
        ClassRelationId::new(r.integer("class_relation_id")?)
            .map_err(|_| invalid("class_relation_id"))?,
    )
    .map_err(invalid_contract_value)
}

pub(super) fn restore(
    sections: &StorageBackupStateSections,
    state: &mut MemoryState,
) -> Result<(), StorageError> {
    macro_rules! restore_map {
        ($section:ident, $field:ident, $decode:ident, $sequence:ident) => {
            state.$field = sections[&StorageBackupStateSection::$section]
                .iter()
                .map(|row| {
                    let r = Row(row);
                    Ok((r.integer("id")?, $decode(&r)?))
                })
                .collect::<Result<_, StorageError>>()?;
            state.$sequence = next_id(&state.$field)?;
        };
    }
    restore_map!(Collections, collections, collection, next_collection_id);
    restore_map!(Classes, classes, class, next_class_id);
    restore_map!(Objects, objects, object, next_object_id);
    restore_map!(
        ClassRelations,
        class_relations,
        class_relation,
        next_class_relation_id
    );
    restore_map!(
        ObjectRelations,
        object_relations,
        object_relation,
        next_object_relation_id
    );
    restore_map!(
        ExportTemplates,
        export_templates,
        export_template,
        next_export_template_id
    );
    restore_map!(
        RemoteTargets,
        remote_targets,
        remote_target,
        next_remote_target_id
    );
    Ok(())
}

fn capture_integrations(
    state: &MemoryState,
    sections: &mut StorageBackupStateSections,
    progress: &mut StorageBackupCaptureProgress,
) -> Result<(), StorageError> {
    sections.insert(
        StorageBackupStateSection::ExportTemplates,
        state
            .export_templates
            .values()
            .map(export_templates)
            .map(|row| capture_row(progress, row))
            .collect::<Result<_, _>>()?,
    );
    sections.insert(
        StorageBackupStateSection::RemoteTargets,
        state
            .remote_targets
            .values()
            .map(remote_targets)
            .map(|row| capture_row(progress, row))
            .collect::<Result<_, _>>()?,
    );
    Ok(())
}

pub(super) fn export_templates(
    value: &StorageExportTemplate,
) -> Result<StorageBackupRow, StorageError> {
    let (meta, collection, name, definition) = value.clone().into_parts();
    let d = definition.into_parts();
    row(
        json!({"id": meta.id().id(), "collection_id": collection.id(), "name": name,
        "description": d.description(), "content_type": d.content_type(), "template": d.template(), "kind": d.kind(),
        "scope_kind": d.scope_kind(), "class_id": d.class_id().map(ClassId::id), "default_query": d.default_query(),
        "include": d.include(), "relation_context": d.relation_context(), "default_missing_data_policy": d.default_missing_data_policy(),
        "default_limits": d.default_limits(), "created_at": meta.created_at(), "updated_at": meta.updated_at(), "revision": meta.revision().get()}),
    )
}

pub(super) fn export_template(r: &Row<'_>) -> Result<StorageExportTemplate, StorageError> {
    let definition = StorageExportTemplateDefinition::new(
        r.text("description")?,
        r.text("content_type")?,
        r.text("template")?,
        r.text("kind")?,
    )
    .with_scope(
        r.optional_text("scope_kind")?,
        r.optional_integer("class_id")?
            .map(ClassId::new)
            .transpose()
            .map_err(|_| invalid("class_id"))?,
    )
    .with_default_query(r.optional_text("default_query")?)
    .with_include(r.optional_value("include").cloned())
    .with_relation_context(r.optional_value("relation_context").cloned())
    .with_default_missing_data_policy(r.optional_text("default_missing_data_policy")?)
    .with_default_limits(r.optional_value("default_limits").cloned());
    Ok(StorageExportTemplate::new(
        r.metadata()?,
        CollectionId::new(r.integer("collection_id")?).map_err(|_| invalid("collection_id"))?,
        r.text("name")?,
        definition,
    ))
}

pub(super) fn remote_targets(
    value: &StorageRemoteTarget,
) -> Result<StorageBackupRow, StorageError> {
    let (meta, collection, name, definition) = value.clone().into_parts();
    let (description, transport, policy) = definition.into_parts();
    let t = transport.into_parts();
    let (class, subjects, enabled) = policy.into_parts();
    row(
        json!({"id": meta.id().id(), "collection_id": collection.id(), "name": name, "description": description,
        "class_id": class.map(ClassId::id), "method": t.method().as_str(), "url_template": t.url_template(),
        "headers_template": t.headers_template(), "body_template": t.body_template(), "auth_config": t.auth_config(),
        "allowed_subject_types": subjects.iter().map(|s| s.as_str()).collect::<Vec<_>>(), "timeout_ms": t.timeout_ms(), "enabled": enabled,
        "created_at": meta.created_at(), "updated_at": meta.updated_at(), "revision": meta.revision().get()}),
    )
}

pub(super) fn remote_target(r: &Row<'_>) -> Result<StorageRemoteTarget, StorageError> {
    let transport = StorageRemoteTargetTransport::try_new(
        r.text("method")?.parse()?,
        r.text("url_template")?,
        r.value("headers_template")?.clone(),
        r.optional_text("body_template")?,
        r.value("auth_config")?.clone(),
        r.integer("timeout_ms")?,
    )
    .map_err(invalid_contract_value)?;
    let subjects = r
        .value("allowed_subject_types")?
        .as_array()
        .ok_or_else(|| invalid("allowed_subject_types"))?
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| invalid("allowed_subject_types"))?
                .parse()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let policy = StorageRemoteTargetPolicy::try_new(
        r.optional_integer("class_id")?
            .map(ClassId::new)
            .transpose()
            .map_err(|_| invalid("class_id"))?,
        subjects,
        r.boolean("enabled")?,
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageRemoteTarget::new(
        r.metadata()?,
        CollectionId::new(r.integer("collection_id")?).map_err(|_| invalid("collection_id"))?,
        r.text("name")?,
        StorageRemoteTargetDefinition::new(r.text("description")?, transport, policy),
    ))
}

pub(super) fn capture(
    state: &MemoryState,
    sections: &mut StorageBackupStateSections,
    progress: &mut StorageBackupCaptureProgress,
) -> Result<(), StorageError> {
    sections.insert(
        StorageBackupStateSection::Collections,
        state
            .collections
            .values()
            .map(collections)
            .map(|row| capture_row(progress, row))
            .collect::<Result<Vec<_>, _>>()?,
    );
    sections.insert(
        StorageBackupStateSection::Classes,
        state
            .classes
            .values()
            .map(classes)
            .map(|row| capture_row(progress, row))
            .collect::<Result<Vec<_>, _>>()?,
    );
    sections.insert(
        StorageBackupStateSection::Objects,
        state
            .objects
            .values()
            .map(objects)
            .map(|row| capture_row(progress, row))
            .collect::<Result<Vec<_>, _>>()?,
    );
    sections.insert(
        StorageBackupStateSection::ClassRelations,
        state
            .class_relations
            .values()
            .map(classrelations)
            .map(|row| capture_row(progress, row))
            .collect::<Result<Vec<_>, _>>()?,
    );
    sections.insert(
        StorageBackupStateSection::ObjectRelations,
        state
            .object_relations
            .values()
            .map(objectrelations)
            .map(|row| capture_row(progress, row))
            .collect::<Result<Vec<_>, _>>()?,
    );

    capture_integrations(state, sections, progress)?;
    Ok(())
}

pub(super) fn collections(value: &StorageCollection) -> Result<StorageBackupRow, StorageError> {
    let collection = value;
    row(serde_json::json!({
        "id": collection.id().id(),
        "name": collection.name(),
        "description": collection.description(),
        "created_at": collection.created_at(),
        "updated_at": collection.updated_at(),
        "parent_collection_id": collection.parent_collection_id().map(CollectionId::id),
        "revision": collection.revision().get(),
    }))
}

pub(super) fn classes(value: &StorageClass) -> Result<StorageBackupRow, StorageError> {
    let class = value;
    row(serde_json::json!({
        "id": class.id().id(),
        "name": class.name(),
        "collection_id": class.collection_id().id(),
        "json_schema": class.json_schema(),
        "validate_schema": class.validates_schema(),
        "description": class.description(),
        "created_at": class.created_at(),
        "updated_at": class.updated_at(),
        "revision": class.revision().get(),
    }))
}

pub(super) fn objects(value: &StorageObject) -> Result<StorageBackupRow, StorageError> {
    let object = value;
    row(serde_json::json!({
        "id": object.id().id(),
        "name": object.name(),
        "collection_id": object.collection_id().id(),
        "class_id": object.class_id().id(),
        "data": object.data(),
        "description": object.description(),
        "created_at": object.created_at(),
        "updated_at": object.updated_at(),
        "revision": object.revision().get(),
    }))
}

pub(super) fn classrelations(
    value: &StorageClassRelation,
) -> Result<StorageBackupRow, StorageError> {
    let relation = value;
    row(serde_json::json!({
        "id": relation.metadata().id().id(),
        "from_class_id": relation.from_class_id().id(),
        "to_class_id": relation.to_class_id().id(),
        "forward_template_alias": relation.forward_template_alias(),
        "reverse_template_alias": relation.reverse_template_alias(),
        "from_max_relations": relation.from_max_relations(),
        "to_max_relations": relation.to_max_relations(),
        "created_at": relation.metadata().created_at(),
        "updated_at": relation.metadata().updated_at(),
        "revision": relation.metadata().revision().get(),
    }))
}

pub(super) fn objectrelations(
    value: &StorageObjectRelation,
) -> Result<StorageBackupRow, StorageError> {
    let relation = value;
    row(serde_json::json!({
        "id": relation.metadata().id().id(),
        "from_object_id": relation.from_object_id().id(),
        "to_object_id": relation.to_object_id().id(),
        "class_relation_id": relation.class_relation_id().id(),
        "created_at": relation.metadata().created_at(),
        "updated_at": relation.metadata().updated_at(),
        "revision": relation.metadata().revision().get(),
    }))
}
