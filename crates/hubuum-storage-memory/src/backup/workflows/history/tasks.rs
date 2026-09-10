use super::*;

pub(super) fn capture(
    state: &MemoryState,
    sections: &mut StorageBackupHistorySections,
) -> Result<(), StorageError> {
    let terminal = |id: i32| {
        state
            .tasks
            .get(&id)
            .is_some_and(|task| task.status.is_terminal())
    };
    let tasks = state.tasks.values().filter(|t| t.status.is_terminal()).map(|t| {
        let mut fields = json!({"id": t.id.id(), "kind": t.kind.as_str(), "status": t.status.as_str(), "submitted_by": t.submitted_by.map(PrincipalId::id),
            "request_hash": t.request_hash, "request_payload": t.request_payload, "summary": t.summary, "total_items": t.progress.total(), "processed_items": t.progress.processed(),
            "success_items": t.progress.succeeded(), "failed_items": t.progress.failed(), "submitted_token_scoped": t.scope_snapshot.scoped(), "submitted_token_scopes": t.scope_snapshot.scopes(),
            "request_redacted_at": t.request_redacted_at, "started_at": t.started_at, "finished_at": t.finished_at, "deleted_at": t.deleted_at, "deleted_by": t.deleted_by.map(PrincipalId::id),
            "created_at": t.created_at, "updated_at": t.updated_at, "attempt_count": t.attempt_count, "initiator_principal_id": t.initiator_principal_id.map(PrincipalId::id)}).as_object().expect("literal object").clone();
        add_trace(&mut fields, t.trace_link.as_ref());
        row(Value::Object(fields))
    }).collect::<Result<_, _>>()?;
    sections.insert(StorageBackupHistorySection::TerminalTasks, tasks);
    sections.insert(StorageBackupHistorySection::ImportResults, state.import_task_results.values().flatten().filter(|r| terminal(r.task_id().id())).map(|r| row(json!({
        "id": r.id().id(), "task_id": r.task_id().id(), "item_ref": r.item_ref(), "entity_kind": r.entity_kind(), "action": r.action(), "identifier": r.identifier(),
        "outcome": r.outcome(), "error": r.error(), "details": r.details(), "created_at": r.created_at()
    }))).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupHistorySection::ExportOutputs, state.export_outputs.values().filter(|o| terminal(o.task_id().id())).map(|o| row(json!({
        "id": state.export_output_ids.get(&o.task_id().id()).copied().unwrap_or(o.task_id().id()), "task_id": o.task_id().id(), "template_name": o.template_name(), "content_type": o.content_type(), "json_output": o.json_output(), "text_output": o.text_output(),
        "meta_json": o.metadata(), "warnings_json": o.warnings(), "warning_count": o.warning_count(), "truncated": o.truncated(), "output_expires_at": o.output_expires_at(),
        "total_duration_ms": o.durations().total_ms(), "query_duration_ms": o.durations().query_ms(), "hydration_duration_ms": o.durations().hydration_ms(), "render_duration_ms": o.durations().render_ms(), "created_at": o.created_at()
    }))).collect::<Result<_, _>>()?);
    sections.insert(
        StorageBackupHistorySection::RemoteCallResults,
        state
            .remote_call_results
            .iter()
            .filter(|r| {
                r.get("task_id")
                    .and_then(Value::as_i64)
                    .and_then(|n| i32::try_from(n).ok())
                    .is_some_and(terminal)
            })
            .cloned()
            .collect(),
    );
    Ok(())
}

pub(super) fn restore(
    sections: &StorageBackupHistorySections,
    state: &mut MemoryState,
) -> Result<(), StorageError> {
    for row in &sections[&StorageBackupHistorySection::TerminalTasks] {
        let r = Row(row);
        let id = TaskId::new(r.integer("id")?).map_err(|_| invalid("id"))?;
        let task = MemoryTaskRecord {
            id,
            kind: StorageTaskKind::from_persisted(r.text("kind")?)
                .ok_or_else(|| invalid("kind"))?,
            status: StorageTaskStatus::from_persisted(r.text("status")?)
                .ok_or_else(|| invalid("status"))?,
            submitted_by: r
                .optional_integer("submitted_by")?
                .map(PrincipalId::new)
                .transpose()
                .map_err(|_| invalid("submitted_by"))?,
            idempotency_key: None,
            request_hash: r.optional_text("request_hash")?,
            request_payload: r.optional_value("request_payload").cloned(),
            summary: r.optional_text("summary")?,
            progress: StorageTaskProgress::try_new(
                r.integer("total_items")?,
                r.integer("processed_items")?,
                r.integer("success_items")?,
                r.integer("failed_items")?,
            )
            .map_err(invalid_contract_value)?,
            scope_snapshot: StorageTaskScopeSnapshot::new(
                None,
                r.boolean("submitted_token_scoped")?,
                r.value("submitted_token_scopes")?.clone(),
            ),
            request_redacted_at: r.optional_time("request_redacted_at")?,
            started_at: r.optional_time("started_at")?,
            finished_at: r.optional_time("finished_at")?,
            deleted_at: r.optional_time("deleted_at")?,
            deleted_by: r
                .optional_integer("deleted_by")?
                .map(PrincipalId::new)
                .transpose()
                .map_err(|_| invalid("deleted_by"))?,
            created_at: r.time("created_at")?,
            updated_at: r.time("updated_at")?,
            lease_expires_at: None,
            claim_token: None,
            attempt_count: r.integer("attempt_count")?,
            initiator_principal_id: r
                .optional_integer("initiator_principal_id")?
                .map(PrincipalId::new)
                .transpose()
                .map_err(|_| invalid("initiator_principal_id"))?,
            trace_link: trace(&r)?,
        };
        task.projection()?;
        state.tasks.insert(id.id(), task);
    }
    for row in &sections[&StorageBackupHistorySection::ImportResults] {
        let r = Row(row);
        let result = StorageImportTaskResult::builder(
            ImportTaskResultId::new(r.integer("id")?).map_err(|_| invalid("id"))?,
            TaskId::new(r.integer("task_id")?).map_err(|_| invalid("task_id"))?,
            r.text("entity_kind")?,
            r.text("action")?,
            r.text("outcome")?,
            r.time("created_at")?,
        )
        .item_ref(r.optional_text("item_ref")?)
        .identifier(r.optional_text("identifier")?)
        .error(r.optional_text("error")?)
        .details(r.optional_value("details").cloned())
        .build();
        state
            .import_task_results
            .entry(result.task_id().id())
            .or_default()
            .push(result);
    }
    for row in &sections[&StorageBackupHistorySection::ExportOutputs] {
        let r = Row(row);
        let text_output = r.optional_text("text_output")?;
        let json_output = if text_output.is_none() {
            Some(r.value("json_output")?.clone())
        } else {
            r.optional_value("json_output").cloned()
        };
        let output = StorageExportOutput::builder(
            TaskId::new(r.integer("task_id")?).map_err(|_| invalid("task_id"))?,
            r.text("content_type")?,
            r.value("meta_json")?.clone(),
            r.value("warnings_json")?.clone(),
            r.time("output_expires_at")?,
            r.time("created_at")?,
        )
        .template_name(r.optional_text("template_name")?)
        .output(json_output, text_output)
        .warning_state(r.integer("warning_count")?, r.boolean("truncated")?)
        .durations(
            StorageTaskDurations::try_new(
                r.integer("total_duration_ms")?,
                r.integer("query_duration_ms")?,
                r.integer("hydration_duration_ms")?,
                r.integer("render_duration_ms")?,
            )
            .map_err(invalid_contract_value)?,
        )
        .try_build()
        .map_err(invalid_contract_value)?;
        state
            .export_output_ids
            .insert(output.task_id().id(), r.integer("id")?);
        state.export_outputs.insert(output.task_id().id(), output);
    }
    state.remote_call_results = sections[&StorageBackupHistorySection::RemoteCallResults].clone();
    state.next_task_id = next_id(&state.tasks)?;
    state.next_import_result_id = state
        .import_task_results
        .values()
        .flatten()
        .map(|v| v.id().id())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid("import result sequence"))?;
    Ok(())
}
