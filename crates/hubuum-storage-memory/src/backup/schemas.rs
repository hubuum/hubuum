use super::*;

pub(super) fn capture(
    state: &MemoryState,
    sections: &mut StorageBackupStateSections,
    progress: &mut StorageBackupCaptureProgress,
) -> Result<(), StorageError> {
    let mut revisions = Vec::new();
    for revision in state.schema_revisions.values() {
        progress.scan_row()?;
        if state
            .classes
            .contains_key(&revision.reference().class_id().id())
        {
            revisions.push(retain_row(progress, row(revision.snapshot()))?);
        }
    }
    sections.insert(StorageBackupStateSection::ClassSchemaRevisions, revisions);
    // Resource capture has already admitted these objects. Count them once,
    // rather than scanning all objects again for every class schema row.
    let mut object_counts = BTreeMap::<i32, usize>::new();
    for object in state.objects.values() {
        *object_counts.entry(object.class_id().id()).or_default() += 1;
    }
    sections.insert(StorageBackupStateSection::ClassSchemaState,state.schema_active.iter().map(|(id,revision)|{
        let last=state.schema_revisions.range((*id,0)..=(*id,i64::MAX)).next_back().map(|((_,revision),_)|*revision).unwrap_or(revision.get());
        row(json!({"class_id":id,"active_revision":revision,"last_revision":last,"object_epoch":state.schema_epochs.get(id).copied().unwrap_or(0),"object_count":object_counts.get(id).copied().unwrap_or(0)}))
    }).map(|row| capture_row(progress, row)).collect::<Result<_,_>>()?);
    sections.insert(StorageBackupStateSection::ObjectSchemaEvidence,state.schema_evidence.iter().map(|(id,evidence)|row(json!({"object_id":id,"class_id":evidence.schema().class_id(),"schema_revision":evidence.schema().revision(),"object_revision":evidence.object_revision(),"valid":evidence.valid(),"validated_at":evidence.validated_at()}))).map(|row| capture_row(progress, row)).collect::<Result<_,_>>()?);
    Ok(())
}

pub(super) fn restore(
    sections: &StorageBackupStateSections,
    state: &mut MemoryState,
    schema_limits: JsonSchemaLimits,
) -> Result<(), StorageError> {
    state.schema_revisions = sections[&StorageBackupStateSection::ClassSchemaRevisions]
        .iter()
        .map(|row| {
            let revision = StorageSchemaRevision::from_snapshot_with_limits(
                row.clone().into_value(),
                schema_limits,
            )
            .map_err(invalid_contract_value)?;
            Ok((
                (
                    revision.reference().class_id().id(),
                    revision.reference().revision().get(),
                ),
                revision,
            ))
        })
        .collect::<Result<_, StorageError>>()?;
    for row in &sections[&StorageBackupStateSection::ClassSchemaState] {
        let r = Row(row);
        let id = r.integer("class_id")?;
        let active = row
            .get("active_revision")
            .and_then(Value::as_i64)
            .ok_or_else(|| invalid("active_revision"))?;
        let epoch = row
            .get("object_epoch")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("object_epoch"))?;
        state.schema_active.insert(
            id,
            SchemaRevision::new(active).map_err(|_| invalid("schema revision"))?,
        );
        state.schema_epochs.insert(id, epoch);
    }
    for row in &sections[&StorageBackupStateSection::ObjectSchemaEvidence] {
        let r = Row(row);
        let reference = SchemaReference::new(
            ClassId::new(r.integer("class_id")?).map_err(|_| invalid("class id"))?,
            SchemaRevision::new(
                row.get("schema_revision")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| invalid("schema revision"))?,
            )
            .map_err(|_| invalid("schema revision"))?,
        );
        let revision = ResourceRevision::new(
            row.get("object_revision")
                .and_then(Value::as_i64)
                .ok_or_else(|| invalid("object revision"))?,
        )
        .map_err(|_| invalid("object revision"))?;
        let at = DateTime::parse_from_rfc3339(r.text("validated_at")?)
            .map_err(|_| invalid("validated_at"))?
            .with_timezone(&Utc);
        state.schema_evidence.insert(
            r.integer("object_id")?,
            StorageSchemaEvidence::new(reference, revision, r.boolean("valid")?, at),
        );
    }
    Ok(())
}

pub(super) fn enqueue_revalidation(state: &mut MemoryState) -> Result<(), StorageError> {
    let targets = state
        .schema_active
        .iter()
        .map(|(class, revision)| {
            ClassId::new(*class)
                .map(|class| SchemaReference::new(class, *revision))
                .map_err(|_| invalid("class id"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for target in targets {
        let active = state
            .schema_revisions
            .get(&(target.class_id().id(), target.revision().get()))
            .ok_or_else(|| invalid("active schema"))?;
        if !active.policy().policy().validates_schema() {
            continue;
        }
        state.enqueue_restored_schema_work(&StorageSchemaWorkRequest::new(
            state
                .classes
                .get(&target.class_id().id())
                .ok_or_else(|| invalid("class"))?
                .collection_id(),
            target,
            StorageSchemaWorkKind::Revalidation,
            EventContext::system(),
        ))?;
    }
    Ok(())
}
