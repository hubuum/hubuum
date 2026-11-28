use super::*;

mod tasks;

impl MemoryHistoryValue {
    fn backup_resource(
        &self,
    ) -> Result<(StorageBackupHistorySection, StorageBackupRow), StorageError> {
        Ok(match self {
            MemoryHistoryValue::Collection(v) => (
                StorageBackupHistorySection::CollectionHistory,
                resources::collections(v)?,
            ),
            MemoryHistoryValue::Class(v) => (
                StorageBackupHistorySection::ClassHistory,
                resources::classes(v)?,
            ),
            MemoryHistoryValue::Object(v) => (
                StorageBackupHistorySection::ObjectHistory,
                resources::objects(v)?,
            ),
            MemoryHistoryValue::ExportTemplate(v) => (
                StorageBackupHistorySection::ExportTemplateHistory,
                resources::export_templates(v)?,
            ),
            MemoryHistoryValue::RemoteTarget(v) => (
                StorageBackupHistorySection::RemoteTargetHistory,
                resources::remote_targets(v)?,
            ),
        })
    }

    pub(crate) fn history_resource_size(&self) -> Result<MemoryHistoryResourceSize, StorageError> {
        let (_, resource) = self.backup_resource()?;
        let encoded = bounded_encoded_bytes(
            &resource,
            u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES),
        )?;
        // A value outside logical-accounting limits may still be stored and backed up.
        let logical = HistoryLogicalBytes::measure_fields_json_v1(resource.fields()).ok();
        Ok(MemoryHistoryResourceSize { encoded, logical })
    }
}

impl MemoryHistoryEntry {
    pub(crate) fn history_metadata(&self) -> impl serde::Serialize + '_ {
        #[derive(serde::Serialize)]
        struct Metadata<'a> {
            logical_identity: &'a StorageIdentityEvidence,
            history_entry_id: HistoryRecordId,
            operation: &'a str,
            valid_from: DateTime<Utc>,
            valid_to: Option<DateTime<Utc>>,
            actor_principal_id: Option<PrincipalId>,
            actor_kind: Option<&'a str>,
            initiator_principal_id: Option<PrincipalId>,
            task_id: Option<TaskId>,
        }
        Metadata {
            logical_identity: &self.logical_identity,
            history_entry_id: self.id,
            operation: self.operation.as_str(),
            valid_from: self.valid_from,
            valid_to: self.valid_to,
            actor_principal_id: self.actor_id,
            actor_kind: self.actor_kind.as_deref(),
            initiator_principal_id: self.initiator_principal_id,
            task_id: self.task_id,
        }
    }

    pub(crate) fn backup_history_row(
        &self,
    ) -> Result<(StorageBackupHistorySection, StorageBackupRow), StorageError> {
        let (section, resource) = self.value.backup_resource()?;
        let Value::Object(mut fields) = resource.into_value() else {
            unreachable!("typed backup row")
        };
        let Value::Object(metadata) = serde_json::to_value(self.history_metadata())
            .map_err(|error| StorageError::internal(error.to_string()))?
        else {
            unreachable!("typed backup row")
        };
        fields.extend(metadata);
        Ok((section, row(Value::Object(fields))?))
    }
}

fn advance_retained_sequence(
    ids: &mut IdSequence,
    rows: &[StorageBackupRow],
    column: &str,
) -> Result<(), StorageError> {
    for row in rows {
        ids.observe(Row(row).integer(column)?);
    }
    Ok(())
}

pub(in crate::backup) fn capture_history(
    state: &MemoryState,
    progress: &mut StorageBackupCaptureProgress,
) -> Result<StorageBackupHistorySections, StorageError> {
    let mut sections = StorageBackupHistorySection::ALL
        .iter()
        .copied()
        .map(|section| (section, Vec::new()))
        .collect::<StorageBackupHistorySections>();
    sections.insert(
        StorageBackupHistorySection::ClassSchemaHistory,
        state
            .schema_history
            .iter()
            .filter_map(|row| {
                match state.raw_history_visible(StorageIdentityHistoryKind::ClassSchema, row) {
                    Ok(true) => Some(capture_row(progress, Ok(row.clone()))),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .collect::<Result<_, _>>()?,
    );
    sections.insert(
        StorageBackupHistorySection::ComputedFieldHistory,
        state
            .computed_history
            .iter()
            .filter(|record| {
                state.history_identity_visible(
                    StorageIdentityHistoryKind::ComputedDefinitions,
                    record.logical_identity(),
                )
            })
            .map(|record| {
                capture_row(
                    progress,
                    serde_json::to_value(record)
                        .map_err(|error| StorageError::internal(error.to_string()))
                        .and_then(row),
                )
            })
            .collect::<Result<_, _>>()?,
    );
    for entry in &state.history {
        if !state.history_entry_visible(entry) {
            continue;
        }
        let (section, row) = entry.backup_history_row()?;
        sections
            .get_mut(&section)
            .expect("complete sections")
            .push(capture_row(progress, Ok(row))?);
    }
    for (&section, rows) in &state.relation_history {
        let family = match section {
            StorageBackupHistorySection::ClassRelationHistory => {
                StorageIdentityHistoryKind::ClassRelation
            }
            StorageBackupHistorySection::ObjectRelationHistory => {
                StorageIdentityHistoryKind::ObjectRelation
            }
            _ => {
                return Err(StorageError::backend_failure(
                    "Invalid relation history section",
                ));
            }
        };
        sections.insert(
            section,
            rows.iter()
                .filter_map(|row| match state.raw_history_visible(family, row) {
                    Ok(true) => Some(capture_row(progress, Ok(row.clone()))),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<_, _>>()?,
        );
    }
    let mut events = Vec::new();
    for recorded in &state.events {
        progress.scan_row()?;
        let (event, before, after) = recorded.as_parts();
        if event.entity_type() == EntityType::Task
            && event.entity_id().is_some_and(|id| {
                state.tasks.get(&id.get()).is_some_and(|task| {
                    !task.status.is_terminal()
                        && recorded
                            .logical_identity()
                            .and_then(|proof| proof.task_reference("/entity_id"))
                            .zip(state.task_identity(task.id).ok())
                            .is_some_and(|(original, current)| original == current)
                })
            })
        {
            continue;
        }
        let mut fields = json!({"id": event.id().get(), "event_id": event.event_id(), "occurred_at": event.occurred_at(), "entity_type": event.entity_type(),
            "entity_id": event.entity_id().map(EventEntityId::get), "entity_name": event.entity_name(), "collection_id": event.collection_id().map(CollectionId::id),
            "action": event.action(), "actor_principal_id": event.actor_user_id().map(PrincipalId::id), "actor_kind": event.actor_kind(),
            "request_id": event.request_id(), "correlation_id": event.correlation_id(), "summary": event.summary(), "before": event.before(), "after": event.after(),
            "metadata": event.metadata(), "schema_version": event.schema_version(), "dispatched_at": state.event_dispatched_at.get(&event.id().get()).copied().unwrap_or(event.occurred_at()),
            "initiator_principal_id": event.provenance().initiator.as_ref().map(|p| p.principal_id.id()), "task_id": event.provenance().task_id.map(TaskId::id),
            "before_revision": before.map(ResourceRevision::get), "after_revision": after.map(ResourceRevision::get)}).as_object().expect("literal object").clone();
        fields.insert(
            "logical_identity".into(),
            serde_json::to_value(
                recorded
                    .logical_identity()
                    .ok_or_else(|| invalid("event identity"))?,
            )
            .map_err(|_| invalid("event identity"))?,
        );
        add_trace(&mut fields, event.trace_link());
        events.push(retain_row(progress, row(Value::Object(fields)))?);
    }
    let event_ids = events
        .iter()
        .filter_map(|event| event.get("id").and_then(Value::as_i64))
        .collect::<BTreeSet<_>>();
    sections.insert(StorageBackupHistorySection::AuditEvents, events);
    let mut deliveries = Vec::new();
    for d in state.event_deliveries.values() {
        progress.scan_row()?;
        if !matches!(
            d.status(),
            EventDeliveryStatus::Succeeded | EventDeliveryStatus::Dead
        ) {
            continue;
        }
        // Deliveries must refer to events retained by the same snapshot.
        if !event_ids.contains(&d.event_id().get()) {
            continue;
        }
        deliveries.push(retain_row(progress, row(json!({
            "logical_identity": state.logical_identities.association(Association::Delivery(d.id()))?,
            "id": d.id().id(), "event_id": d.event_id().get(), "subscription_id": d.subscription_id().id(), "status": d.status().as_str(), "purpose": d.purpose(), "deferred_reason": d.deferred_reason(), "attempts": d.attempts(),
            "next_attempt_at": d.next_attempt_at(), "last_error": d.last_error(), "created_at": d.created_at(), "updated_at": d.updated_at()
        })))?);
    }
    sections.insert(
        StorageBackupHistorySection::TerminalEventDeliveries,
        deliveries,
    );
    tasks::capture(state, &mut sections, progress)?;
    Ok(sections)
}

fn add_trace(fields: &mut Map<String, Value>, trace: Option<&TraceLink>) {
    if let Some(trace) = trace {
        fields.extend(json!({"trace_id": trace.trace_id(), "trace_span_id": trace.span_id(), "trace_flags": trace.trace_flags(), "trace_context_version": trace.version()}).as_object().expect("literal object").clone());
    }
}

fn trace(r: &Row<'_>) -> Result<Option<TraceLink>, StorageError> {
    if r.optional_value("trace_id").is_none() {
        return Ok(None);
    }
    TraceLink::new(
        r.text("trace_id")?,
        r.text("trace_span_id")?,
        u8::try_from(r.integer("trace_flags")?).map_err(|_| invalid("trace_flags"))?,
        u8::try_from(r.integer("trace_context_version")?)
            .map_err(|_| invalid("trace_context_version"))?,
    )
    .map(Some)
    .map_err(|_| invalid("trace link"))
}

pub(in crate::backup) fn restore_history(
    mut sections: StorageBackupHistorySections,
    state: &mut MemoryState,
) -> Result<(), StorageError> {
    for (&section, rows) in &mut sections {
        for row in rows {
            row.normalize_legacy_history(section);
        }
    }
    // Restored allocators must include deleted resources retained in history.
    for (section, next) in [
        (
            StorageBackupHistorySection::CollectionHistory,
            &mut state.collection_ids,
        ),
        (
            StorageBackupHistorySection::ClassHistory,
            &mut state.class_ids,
        ),
        (
            StorageBackupHistorySection::ObjectHistory,
            &mut state.object_ids,
        ),
        (
            StorageBackupHistorySection::ClassRelationHistory,
            &mut state.class_relation_ids,
        ),
        (
            StorageBackupHistorySection::ObjectRelationHistory,
            &mut state.object_relation_ids,
        ),
        (
            StorageBackupHistorySection::ExportTemplateHistory,
            &mut state.export_template_ids,
        ),
        (
            StorageBackupHistorySection::RemoteTargetHistory,
            &mut state.remote_target_ids,
        ),
    ] {
        advance_retained_sequence(next, &sections[&section], "id")?;
    }
    for section in [
        StorageBackupHistorySection::ClassSchemaHistory,
        StorageBackupHistorySection::ComputedFieldHistory,
    ] {
        advance_retained_sequence(&mut state.class_ids, &sections[&section], "class_id")?;
    }
    state.schema_history = sections[&StorageBackupHistorySection::ClassSchemaHistory].clone();
    state
        .schema_history
        .sort_by_key(|row| row.get("id").and_then(Value::as_i64));
    state.computed_history = sections[&StorageBackupHistorySection::ComputedFieldHistory]
        .iter()
        .map(|row| {
            serde_json::from_value(row.clone().into_value()).map_err(|error| {
                StorageError::backend_failure(format!("Invalid computed history: {error}"))
            })
        })
        .collect::<Result<_, _>>()?;
    state
        .computed_history
        .sort_by_key(StorageComputedFieldHistory::history_entry_id);
    if let Some(retained_id) = state
        .computed_history
        .iter()
        .flat_map(|record| record.definitions())
        .map(|field| field.id().id())
        .max()
    {
        state.computed_field_ids.observe(retained_id);
    }
    state.history.clear();
    for section in [
        StorageBackupHistorySection::CollectionHistory,
        StorageBackupHistorySection::ClassHistory,
        StorageBackupHistorySection::ObjectHistory,
        StorageBackupHistorySection::ExportTemplateHistory,
        StorageBackupHistorySection::RemoteTargetHistory,
    ] {
        for row in &sections[&section] {
            let r = Row(row);
            let value = match section {
                StorageBackupHistorySection::CollectionHistory => {
                    MemoryHistoryValue::Collection(resources::collection(&r)?)
                }
                StorageBackupHistorySection::ClassHistory => {
                    MemoryHistoryValue::Class(resources::class(&r)?)
                }
                StorageBackupHistorySection::ObjectHistory => {
                    MemoryHistoryValue::Object(resources::object(&r)?)
                }
                StorageBackupHistorySection::ExportTemplateHistory => {
                    MemoryHistoryValue::ExportTemplate(resources::export_template(&r)?)
                }
                StorageBackupHistorySection::RemoteTargetHistory => {
                    MemoryHistoryValue::RemoteTarget(resources::remote_target(&r)?)
                }
                _ => unreachable!("temporal resource selection"),
            };
            state.history.push(MemoryHistoryEntry {
                scan_position: 0,
                resource_size: value.history_resource_size()?,
                logical_identity: serde_json::from_value(r.value("logical_identity")?.clone())
                    .map_err(|_| invalid("logical_identity"))?,
                id: HistoryRecordId::new(r.number("history_entry_id")?)
                    .map_err(|_| invalid("history_entry_id"))?,
                value,
                operation: match r.text("operation")? {
                    "create" => StorageHistoryOperation::Create,
                    "update" => StorageHistoryOperation::Update,
                    "delete" => StorageHistoryOperation::Delete,
                    _ => return Err(invalid("operation")),
                },
                valid_from: r.time("valid_from")?,
                valid_to: r.optional_time("valid_to")?,
                actor_id: r
                    .optional_integer("actor_principal_id")?
                    .map(PrincipalId::new)
                    .transpose()
                    .map_err(|_| invalid("actor_principal_id"))?,
                actor_kind: r.optional_text("actor_kind")?,
                initiator_principal_id: r
                    .optional_integer("initiator_principal_id")?
                    .map(PrincipalId::new)
                    .transpose()
                    .map_err(|_| invalid("initiator_principal_id"))?,
                task_id: r
                    .optional_integer("task_id")?
                    .map(TaskId::new)
                    .transpose()
                    .map_err(|_| invalid("task_id"))?,
            });
        }
    }
    state
        .history
        .sort_by_key(|entry| (entry.valid_from, entry.id));
    // Preserve chronological reads while assigning a unique position across native tables.
    for (index, entry) in state.history.iter_mut().enumerate() {
        entry.scan_position = index as u64 + 1;
    }
    for section in [
        StorageBackupHistorySection::ClassRelationHistory,
        StorageBackupHistorySection::ObjectRelationHistory,
    ] {
        let mut rows = sections[&section].clone();
        rows.sort_by_key(|row| row.get("history_entry_id").and_then(Value::as_i64));
        state.relation_history.insert(section, rows);
    }
    let mut highest =
        i64::try_from(state.history.len()).map_err(|_| invalid("history sequence"))?;
    for (section, rows) in &sections {
        use StorageBackupHistorySection::*;
        let column = match section {
            ClassSchemaHistory => "id",
            ComputedFieldHistory
            | CollectionHistory
            | ClassHistory
            | ObjectHistory
            | ClassRelationHistory
            | ObjectRelationHistory
            | ExportTemplateHistory
            | RemoteTargetHistory => "history_entry_id",
            _ => continue,
        };
        for row in rows {
            let id = HistoryRecordId::new(Row(row).number(column)?).map_err(|_| invalid(column))?;
            highest = highest.max(id.id());
        }
    }
    state.history_ids.observe(highest);
    state.events.clear();
    state.task_events.clear();
    for row in &sections[&StorageBackupHistorySection::AuditEvents] {
        let r = Row(row);
        let actor = r
            .optional_integer("actor_principal_id")?
            .map(PrincipalId::new)
            .transpose()
            .map_err(|_| invalid("actor_principal_id"))?;
        let initiator = r
            .optional_integer("initiator_principal_id")?
            .map(PrincipalId::new)
            .transpose()
            .map_err(|_| invalid("initiator_principal_id"))?;
        let provenance = Provenance {
            actor: ProvenanceActor {
                kind: Some(r.text("actor_kind")?.to_string()),
                principal: actor.map(|principal_id| ProvenancePrincipal {
                    principal_id,
                    name: None,
                }),
            },
            initiator: initiator.map(|principal_id| ProvenancePrincipal {
                principal_id,
                name: None,
            }),
            task_id: r
                .optional_integer("task_id")?
                .map(TaskId::new)
                .transpose()
                .map_err(|_| invalid("task_id"))?,
        };
        let envelope = EventEnvelope::builder()
            .id(EventSequence::new(r.number("id")?).map_err(|_| invalid("id"))?)
            .event_id(
                r.text("event_id")?
                    .parse()
                    .map_err(|_| invalid("event_id"))?,
            )
            .occurred_at(r.time("occurred_at")?)
            .entity_type(
                serde_json::from_value(r.value("entity_type")?.clone())
                    .map_err(|_| invalid("entity_type"))?,
            )
            .entity_id(
                r.optional_integer("entity_id")?
                    .map(EventEntityId::new)
                    .transpose()
                    .map_err(|_| invalid("entity_id"))?,
            )
            .entity_name(r.optional_text("entity_name")?)
            .collection_id(
                r.optional_integer("collection_id")?
                    .map(CollectionId::new)
                    .transpose()
                    .map_err(|_| invalid("collection_id"))?,
            )
            .action(
                serde_json::from_value(r.value("action")?.clone())
                    .map_err(|_| invalid("action"))?,
            )
            .actor_user_id(actor)
            .actor_kind(
                serde_json::from_value(r.value("actor_kind")?.clone())
                    .map_err(|_| invalid("actor_kind"))?,
            )
            .provenance(provenance)
            .request_id(
                r.optional_text("request_id")?
                    .map(|v| v.parse())
                    .transpose()
                    .map_err(|_| invalid("request_id"))?,
            )
            .correlation_id(
                r.optional_text("correlation_id")?
                    .map(CorrelationId::new)
                    .transpose()
                    .map_err(|_| invalid("correlation_id"))?,
            )
            .trace_link(trace(&r)?)
            .summary(r.text("summary")?.to_string())
            .before(r.optional_value("before").cloned())
            .after(r.optional_value("after").cloned())
            .metadata(r.value("metadata")?.clone())
            .schema_version(r.integer("schema_version")?)
            .try_build()
            .map_err(|_| invalid("event"))?;
        let revision = |field| {
            r.optional_value(field)
                .map(|_| ResourceRevision::new(r.number(field)?).map_err(|_| invalid(field)))
                .transpose()
        };
        state
            .event_dispatched_at
            .insert(envelope.id().get(), r.time("dispatched_at")?);
        let identity = r.identity()?;
        state.index_task_event(&envelope, &identity)?;
        state.events.push(
            StorageRecordedEvent::new(
                envelope,
                revision("before_revision")?,
                revision("after_revision")?,
            )
            .with_logical_identity(identity),
        );
    }
    for events in state.task_events.values_mut() {
        events.sort_by_key(|event| event.id().get());
    }
    let last_event_sequence = state
        .events
        .iter()
        .map(|e| e.clone().into_parts().0.id().get())
        .max()
        .unwrap_or(0);
    state.event_sequences.observe(last_event_sequence);
    state.fanout_event_cursor = last_event_sequence;
    for row in &sections[&StorageBackupHistorySection::TerminalEventDeliveries] {
        let r = Row(row);
        let value = StorageEventDelivery::builder(
            EventDeliveryId::new(r.number("id")?).map_err(|_| invalid("id"))?,
            EventSequence::new(r.number("event_id")?).map_err(|_| invalid("event_id"))?,
            EventSubscriptionId::new(r.integer("subscription_id")?)
                .map_err(|_| invalid("subscription_id"))?,
            r.text("status")?.parse().map_err(|_| invalid("status"))?,
            r.time("next_attempt_at")?,
            r.time("created_at")?,
            r.time("updated_at")?,
        )
        .purpose(
            serde_json::from_value(r.value("purpose")?.clone()).map_err(|_| invalid("purpose"))?,
        )
        .deferred_reason(r.optional_text("deferred_reason")?)
        .attempts(r.integer("attempts")?)
        .last_error(r.optional_text("last_error")?)
        .try_build()
        .map_err(invalid_contract_value)?;
        state
            .logical_identities
            .restore_association(Association::Delivery(value.id()), r.identity()?);
        state.event_deliveries.store_unclaimed(value);
    }
    let last_id = state
        .event_deliveries
        .values()
        .next_back()
        .map(|delivery| delivery.id().id())
        .unwrap_or(0);
    state.delivery_ids.observe(last_id);
    tasks::restore(&sections, state)?;
    Ok(())
}
