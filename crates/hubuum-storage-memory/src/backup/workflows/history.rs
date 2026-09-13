use super::*;

mod tasks;

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
            .map(|row| capture_row(progress, Ok(row.clone())))
            .collect::<Result<_, _>>()?,
    );
    for entry in &state.history {
        let (section, snapshot) = match &entry.value {
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
        };
        let mut fields = snapshot.fields().clone();
        fields.extend(json!({"history_entry_id": entry.id.id(), "operation": entry.operation.as_str(),
            "valid_from": entry.valid_from, "valid_to": entry.valid_to, "actor_principal_id": entry.actor_id.map(PrincipalId::id),
            "actor_kind": entry.actor_kind, "initiator_principal_id": entry.initiator_principal_id.map(PrincipalId::id), "task_id": entry.task_id.map(TaskId::id)}).as_object().expect("literal object").clone());
        sections
            .get_mut(&section)
            .expect("complete sections")
            .push(capture_row(progress, row(Value::Object(fields)))?);
    }
    for (&section, rows) in &state.relation_history {
        sections.insert(
            section,
            rows.iter()
                .map(|row| capture_row(progress, Ok(row.clone())))
                .collect::<Result<_, _>>()?,
        );
    }
    let mut events = Vec::new();
    for recorded in &state.events {
        progress.scan_row()?;
        let (event, before, after) = recorded.as_parts();
        if event.entity_type() == EntityType::Task
            && !event.entity_id().is_some_and(|id| {
                state
                    .tasks
                    .get(&id.get())
                    .is_some_and(|task| task.status.is_terminal())
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
            "id": d.id().id(), "event_id": d.event_id().get(), "subscription_id": d.subscription_id().id(), "status": d.status().as_str(), "attempts": d.attempts(),
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
    state.schema_history = sections[&StorageBackupHistorySection::ClassSchemaHistory].clone();
    state
        .schema_history
        .sort_by_key(|row| row.get("id").and_then(Value::as_i64));
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
    for section in [
        StorageBackupHistorySection::ClassRelationHistory,
        StorageBackupHistorySection::ObjectRelationHistory,
    ] {
        state
            .relation_history
            .insert(section, sections[&section].clone());
    }
    state.next_history_id = sections
        .iter()
        .filter(|(s, _)| {
            matches!(
                s,
                StorageBackupHistorySection::CollectionHistory
                    | StorageBackupHistorySection::ClassHistory
                    | StorageBackupHistorySection::ObjectHistory
                    | StorageBackupHistorySection::ClassRelationHistory
                    | StorageBackupHistorySection::ObjectRelationHistory
                    | StorageBackupHistorySection::ExportTemplateHistory
                    | StorageBackupHistorySection::RemoteTargetHistory
            )
        })
        .flat_map(|(_, rows)| rows)
        .map(|r| Row(r).number("history_entry_id"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid("history sequence"))?;
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
        state.index_task_event(&envelope)?;
        state.events.push(StorageRecordedEvent::new(
            envelope,
            revision("before_revision")?,
            revision("after_revision")?,
        ));
    }
    for events in state.task_events.values_mut() {
        events.sort_by_key(|event| event.id().get());
    }
    state.next_event_sequence = state
        .events
        .iter()
        .map(|e| e.clone().into_parts().0.id().get())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid("event sequence"))?;
    state.fanout_event_cursor = state.next_event_sequence - 1;
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
        .attempts(r.integer("attempts")?)
        .last_error(r.optional_text("last_error")?)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_deliveries.insert(value.id().id(), value);
    }
    state.next_event_delivery_id = state
        .event_deliveries
        .keys()
        .next_back()
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid("delivery sequence"))?;
    tasks::restore(&sections, state)?;
    Ok(())
}
