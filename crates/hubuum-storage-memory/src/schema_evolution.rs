use super::*;
use chrono::SubsecRound;
use serde_json::json;
use std::io::{self, Write};
use std::time::Instant;

struct BoundedSize {
    bytes: usize,
    limit: usize,
}
impl Write for BoundedSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        if self.bytes > self.limit {
            return Err(io::Error::other("object exceeds byte budget"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn object_size(value: &serde_json::Value, limit: usize) -> usize {
    let mut size = BoundedSize { bytes: 0, limit };
    let _ = serde_json::to_writer(&mut size, value);
    size.bytes
}

struct ValidationObject {
    id: ObjectId,
    revision: ResourceRevision,
    collection_id: CollectionId,
    name: String,
}
impl ValidationObject {
    fn new(object: &StorageObject) -> Self {
        Self {
            id: object.id(),
            revision: object.revision(),
            collection_id: object.collection_id(),
            name: object.name().to_owned(),
        }
    }
    fn id(&self) -> ObjectId {
        self.id
    }
    fn revision(&self) -> ResourceRevision {
        self.revision
    }
    fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    fn name(&self) -> &str {
        &self.name
    }
}

impl MemoryState {
    // Copy only lifecycle metadata for this class. Object documents and their
    // audit/history payloads never enter a schema mutation's staged delta.
    fn schema_mutation_delta(&self, class_id: ClassId) -> Self {
        let mut delta = Self::new();
        delta.history.clear();
        delta.next_event_sequence = self.next_event_sequence;
        delta.next_task_id = self.next_task_id;
        delta.next_history_id = self.next_history_id;
        if let Some(class) = self.classes.get(&class_id.id()) {
            delta.classes.insert(class_id.id(), class.clone());
        }
        delta.schema_revisions = self
            .schema_revisions
            .range((class_id.id(), 0)..=(class_id.id(), i64::MAX))
            .map(|(key, revision)| (*key, revision.clone()))
            .collect();
        if let Some(active) = self.schema_active.get(&class_id.id()) {
            delta.schema_active.insert(class_id.id(), *active);
        }
        if let Some(epoch) = self.schema_epochs.get(&class_id.id()) {
            delta.schema_epochs.insert(class_id.id(), *epoch);
        }
        if let Some(last) = self.schema_history.last() {
            delta.schema_history.push(last.clone());
        }
        for work in self
            .schema_work
            .values()
            .filter(|work| work.target().class_id() == class_id)
        {
            delta.schema_work.insert(work.task_id().id(), work.clone());
            if let Some(task) = self.tasks.get(&work.task_id().id()) {
                delta.tasks.insert(task.id.id(), task.clone());
            }
        }
        if let Some(entry) = self.history.iter().rev().find(|entry| entry.valid_to.is_none() && matches!(&entry.value, MemoryHistoryValue::Class(class) if class.id() == class_id)) { delta.history.push(entry.clone()); }
        if let Some((key, definition)) = self.computed_fields.iter().find(|(_, definition)| {
            definition.class_id() == class_id
                && definition.visibility() == StorageComputedFieldVisibility::Shared
        }) {
            delta.computed_fields.insert(*key, definition.clone());
        }
        if let Some(state) = self.computation_states.get(&class_id.id()) {
            delta
                .computation_states
                .insert(class_id.id(), state.clone());
        }
        delta
    }

    fn commit_schema_delta(&mut self, delta: Self) {
        self.next_event_sequence = delta.next_event_sequence;
        self.next_task_id = delta.next_task_id;
        self.next_history_id = delta.next_history_id;
        self.classes.extend(delta.classes);
        self.schema_revisions.extend(delta.schema_revisions);
        self.schema_active.extend(delta.schema_active);
        self.schema_epochs.extend(delta.schema_epochs);
        self.schema_work.extend(delta.schema_work);
        self.tasks.extend(delta.tasks);
        self.computation_states.extend(delta.computation_states);
        self.computed_rebuild_tasks
            .extend(delta.computed_rebuild_tasks);
        for entry in delta.history {
            if let Some(previous) = self
                .history
                .iter_mut()
                .find(|previous| previous.id == entry.id)
            {
                *previous = entry;
            } else {
                self.history.push(entry);
            }
        }
        let last = self
            .schema_history
            .last()
            .and_then(|row| row.get("id"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        self.schema_history
            .extend(delta.schema_history.into_iter().filter(|row| {
                row.get("id")
                    .and_then(serde_json::Value::as_i64)
                    .is_some_and(|id| id > last)
            }));
        self.events.extend(delta.events);
        for (id, events) in delta.task_events {
            self.task_events.entry(id).or_default().extend(events);
        }
    }

    fn schema_object_boundary(&self, class_id: ClassId) -> (usize, i32) {
        let objects = self
            .objects
            .values()
            .filter(|object| object.class_id() == class_id);
        (
            objects.clone().count(),
            objects.map(|object| object.id().id()).max().unwrap_or(0),
        )
    }

    fn check_schema_collection(
        &self,
        class_id: ClassId,
        expected: CollectionId,
    ) -> Result<(), StorageError> {
        if !self
            .classes
            .get(&class_id.id())
            .is_some_and(|class| class.collection_id() == expected)
        {
            return Err(StorageError::not_found(
                "Class was not found in the authorized collection",
            ));
        }
        Ok(())
    }

    fn active_schema(&self, class_id: ClassId) -> Result<&StorageSchemaRevision, StorageError> {
        if !self.classes.contains_key(&class_id.id()) {
            return Err(StorageError::not_found("Class was not found"));
        }
        let revision = self
            .schema_active
            .get(&class_id.id())
            .ok_or_else(|| StorageError::internal("Class schema state is missing"))?;
        self.schema_revisions
            .get(&(class_id.id(), revision.get()))
            .ok_or_else(|| StorageError::internal("Active schema revision is missing"))
    }

    fn schema_state(&self, class_id: ClassId) -> Result<StorageClassSchemaState, StorageError> {
        let active = self.active_schema(class_id)?;
        let mut counts = StorageComplianceCounts::default();
        for object in self
            .objects
            .values()
            .filter(|object| object.class_id() == class_id)
        {
            counts.observe(StorageSchemaEvidence::effective_status(
                self.schema_evidence.get(&object.id().id()),
                active,
                object.revision(),
            ));
        }
        Ok(StorageClassSchemaState::new(
            active.clone(),
            counts,
            self.schema_epochs.get(&class_id.id()).copied().unwrap_or(0),
        ))
    }

    fn schema_event(
        &mut self,
        class_id: ClassId,
        action: Action,
        context: &EventContext,
        metadata: serde_json::Value,
    ) -> Result<StorageAuditReceipt, StorageError> {
        let class = self
            .classes
            .get(&class_id.id())
            .ok_or_else(|| StorageError::not_found("Class was not found"))?
            .clone();
        let schema_revision = metadata
            .get("schema")
            .and_then(|value| value.get("revision"))
            .and_then(serde_json::Value::as_i64)
            .or_else(|| {
                metadata
                    .get("schema_revision")
                    .and_then(serde_json::Value::as_i64)
            })
            .or_else(|| {
                self.schema_active
                    .get(&class_id.id())
                    .map(|revision| revision.get())
            });
        if (action == Action::Created
            || metadata.get("status").is_some()
            || metadata.get("activation_policy").is_some())
            && let Some(number) = schema_revision
        {
            self.record_schema_history(
                class_id,
                number,
                if action == Action::Created {
                    "create"
                } else {
                    "update"
                },
                context,
            )?;
        }
        let snapshot = json!({"id": class.id(), "revision": class.revision(), "schema": metadata});
        let before = (action != Action::Created).then(|| snapshot.clone());
        let after = (action != Action::Deleted).then_some(snapshot);
        let document =
            AuditDocument::try_new("Class schema lifecycle changed", before, after, metadata)
                .map_err(schema_error)?;
        append_memory_event!(
            self,
            EntityType::ClassSchema,
            class_id.id(),
            Some(class.name()),
            Some(class.collection_id()),
            action,
            context,
            document,
            (action != Action::Created).then_some(class.revision()),
            (action != Action::Deleted).then_some(class.revision())
        )
    }

    fn record_schema_history(
        &mut self,
        class_id: ClassId,
        number: i64,
        operation: &str,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        let revision = self
            .schema_revisions
            .get(&(class_id.id(), number))
            .ok_or_else(|| StorageError::not_found("Schema revision was not found"))?;
        let id = self
            .schema_history
            .last()
            .and_then(|row| row.get("id"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Schema history identity exhausted"))?;
        self.schema_history.push(StorageBackupRow::try_from_value(json!({"id":id,"class_id":class_id,"revision":number,"snapshot":revision.snapshot(),"operation":operation,"occurred_at":Utc::now().trunc_subsecs(6),"actor_principal_id":context.actor_user_id(),"task_id":context.task_id()})).map_err(schema_error)?);
        Ok(())
    }

    pub(super) fn schema_delete_class(
        &mut self,
        class: &StorageClass,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        self.schema_event(
            class.id(),
            Action::Deleted,
            context,
            json!({"active_revision": self.schema_active.get(&class.id().id())}),
        )?;
        let revisions = self
            .schema_revisions
            .range((class.id().id(), 0)..=(class.id().id(), i64::MAX))
            .map(|((_, number), _)| *number)
            .collect::<Vec<_>>();
        for number in revisions {
            self.record_schema_history(class.id(), number, "delete", context)?;
        }
        self.schema_active.remove(&class.id().id());
        self.schema_epochs.remove(&class.id().id());
        let task_ids = self
            .schema_work
            .values()
            .filter(|work| {
                work.target().class_id() == class.id()
                    && work.status() == StorageSchemaWorkStatus::Running
            })
            .map(StorageSchemaWork::task_id)
            .collect::<Vec<_>>();
        for task_id in task_ids {
            self.finish_schema_task(task_id, StorageSchemaWorkStatus::Cancelled)?;
        }
        Ok(())
    }

    pub(super) fn schema_record_class(
        &mut self,
        class: &StorageClass,
        operation: StorageHistoryOperation,
        context: &EventContext,
        policy: StorageValidatedSchemaPolicy,
    ) -> Result<(), StorageError> {
        if self
            .schema_active
            .get(&class.id().id())
            .and_then(|revision| {
                self.schema_revisions
                    .get(&(class.id().id(), revision.get()))
            })
            .is_some_and(|revision| revision.policy().policy() == class.schema_policy())
        {
            return Ok(());
        }
        let next = self
            .schema_revisions
            .range((class.id().id(), 0)..=(class.id().id(), i64::MAX))
            .next_back()
            .map(|(_, revision)| revision.reference().revision().checked_advance())
            .transpose()
            .map_err(|error| StorageError::internal(error.to_string()))?
            .unwrap_or(SchemaRevision::INITIAL);
        if let Some(old) = self.schema_active.get(&class.id().id()).copied() {
            let previous = self
                .schema_revisions
                .get_mut(&(class.id().id(), old.get()))
                .expect("active revision exists");
            *previous = previous
                .clone()
                .restore_lifecycle(
                    StorageSchemaRevisionStatus::Retired,
                    previous.activated_at(),
                    previous.activation_policy(),
                )
                .map_err(schema_error)?;
            self.record_schema_history(class.id(), old.get(), "update", context)?;
        }
        let revision = StorageSchemaRevision::staged(
            SchemaReference::new(class.id(), next),
            policy,
            context,
            Utc::now(),
        )
        .restore_lifecycle(
            StorageSchemaRevisionStatus::Active,
            Some(Utc::now()),
            Some(StorageSchemaActivationPolicy::RejectIncompatible),
        )
        .map_err(schema_error)?;
        self.schema_revisions
            .insert((class.id().id(), next.get()), revision);
        self.schema_active.insert(class.id().id(), next);
        self.schema_epochs.entry(class.id().id()).or_insert(0);
        let dependent_task = if operation == StorageHistoryOperation::Update
            && self.computed_fields.values().any(|definition| {
                definition.class_id() == class.id()
                    && definition.visibility() == StorageComputedFieldVisibility::Shared
            }) {
            let work = self.enqueue_schema_work(&StorageSchemaWorkRequest::new(
                class.collection_id(),
                SchemaReference::new(class.id(), next),
                StorageSchemaWorkKind::Revalidation,
                context.clone(),
            ))?;
            self.invalidate_schema_dependency(&work)?
        } else {
            None
        };
        self.schema_event(class.id(), Action::Created, context, json!({"schema_revision":next,"status":"active","activation_policy":"reject_incompatible","dependent_rebuild_task_id":dependent_task}))?;
        Ok(())
    }

    pub(super) fn schema_record_object(
        &mut self,
        object: &StorageObject,
        operation: StorageHistoryOperation,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        let epoch = self
            .schema_epochs
            .entry(object.class_id().id())
            .or_insert(0);
        *epoch = epoch
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Class object epoch exhausted"))?;
        if operation == StorageHistoryOperation::Delete {
            self.schema_evidence.remove(&object.id().id());
            return Ok(());
        }
        let active = self.active_schema(object.class_id())?.clone();
        let status = active.policy().inspect(object.data());
        self.store_schema_result(&ValidationObject::new(object), &active, status, context)
    }

    fn store_schema_result(
        &mut self,
        object: &ValidationObject,
        active: &StorageSchemaRevision,
        status: StorageComplianceStatus,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        let before = self.schema_evidence.get(&object.id().id()).cloned();
        let before_status =
            StorageSchemaEvidence::effective_status(before.as_ref(), active, object.revision());
        if before_status == status
            && before.as_ref().is_some_and(|evidence| {
                evidence.schema() == active.reference()
                    && evidence.object_revision() == object.revision()
            })
        {
            return Ok(());
        }
        if matches!(
            status,
            StorageComplianceStatus::Valid | StorageComplianceStatus::Invalid
        ) {
            self.schema_evidence.insert(
                object.id().id(),
                StorageSchemaEvidence::new(
                    active.reference(),
                    object.revision(),
                    status == StorageComplianceStatus::Valid,
                    Utc::now(),
                ),
            );
        } else {
            self.schema_evidence.remove(&object.id().id());
        }
        if before.is_none()
            && status == StorageComplianceStatus::NotRequired
            && context.task_id().is_none()
        {
            return Ok(());
        }
        let previous = json!({"id":object.id(),"revision":object.revision(),"validation_status":before_status,"evidence":before});
        let next = json!({"id":object.id(),"revision":object.revision(),"schema":active.reference(),"validation_status":status});
        let metadata = json!({"schema":active.reference(),"object_revision":object.revision(),"validation_status":status,"category":if status==StorageComplianceStatus::Invalid {Some("schema_mismatch")} else {None}});
        let action = match status {
            StorageComplianceStatus::Invalid => Action::Failed,
            StorageComplianceStatus::Valid => Action::Succeeded,
            _ => Action::Updated,
        };
        let document = AuditDocument::try_new(
            "Object schema compliance changed",
            Some(previous),
            Some(next),
            metadata,
        )
        .map_err(schema_error)?;
        append_memory_event!(
            self,
            EntityType::ObjectValidation,
            object.id().id(),
            Some(object.name()),
            Some(object.collection_id()),
            action,
            context,
            document,
            Some(object.revision()),
            Some(object.revision())
        )?;
        Ok(())
    }

    pub(super) fn enqueue_schema_work(
        &mut self,
        request: &StorageSchemaWorkRequest,
    ) -> Result<StorageSchemaWork, StorageError> {
        let (total, upper) = self.schema_object_boundary(request.target().class_id());
        self.enqueue_schema_work_with_boundary(request, total, upper, true)
    }

    pub(super) fn enqueue_restored_schema_work(
        &mut self,
        request: &StorageSchemaWorkRequest,
    ) -> Result<StorageSchemaWork, StorageError> {
        let (total, upper) = self.schema_object_boundary(request.target().class_id());
        self.enqueue_schema_work_with_boundary(request, total, upper, false)
    }

    fn enqueue_schema_work_with_boundary(
        &mut self,
        request: &StorageSchemaWorkRequest,
        total: usize,
        upper: i32,
        record_queue_event: bool,
    ) -> Result<StorageSchemaWork, StorageError> {
        if let Some(work) = self.schema_work.values().find(|work| {
            work.target() == request.target()
                && work.kind() == request.kind()
                && work.status() == StorageSchemaWorkStatus::Running
                && self
                    .tasks
                    .get(&work.task_id().id())
                    .is_some_and(|task| !task.status.is_terminal())
        }) {
            return Ok(work.clone());
        }
        let class_id = request.target().class_id();
        let task_id = TaskId::new(self.next_task_id).map_err(schema_error)?;
        self.next_task_id += 1;
        let work = StorageSchemaWork::start(
            task_id,
            request,
            self.schema_epochs.get(&class_id.id()).copied().unwrap_or(0),
            upper,
            self.active_schema(class_id)?.reference(),
        );
        let now = Utc::now();
        let task = MemoryTaskRecord {
            id: task_id,
            kind: StorageTaskKind::SchemaValidation,
            status: StorageTaskStatus::Queued,
            submitted_by: request.context().actor_user_id(),
            idempotency_key: None,
            request_hash: None,
            request_payload: Some(
                json!({"class_id":class_id,"schema_revision":request.target().revision(),"kind":request.kind()}),
            ),
            summary: None,
            progress: StorageTaskProgress::try_new(
                i32::try_from(total).unwrap_or(i32::MAX),
                0,
                0,
                0,
            )
            .map_err(schema_error)?,
            scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
            deleted_at: None,
            deleted_by: None,
            created_at: now,
            updated_at: now,
            lease_expires_at: None,
            attempt_count: 0,
            initiator_principal_id: request
                .context()
                .actor_user_id()
                .or(request.context().initiator_user_id()),
            trace_link: request.context().trace_link().cloned(),
            claim_token: None,
        };
        self.tasks.insert(task_id.id(), task);
        self.schema_work.insert(task_id.id(), work.clone());
        if record_queue_event {
            self.append_task_event_record(
                task_id,
                StorageTaskEventInput::new("queued", "Schema validation queued"),
            )?;
        }
        Ok(work)
    }

    fn finish_schema_task(
        &mut self,
        task_id: TaskId,
        status: StorageSchemaWorkStatus,
    ) -> Result<(), StorageError> {
        let work = self
            .schema_work
            .get_mut(&task_id.id())
            .ok_or_else(|| StorageError::not_found("Schema work was not found"))?;
        work.finish(
            status,
            self.schema_epochs
                .get(&work.target().class_id().id())
                .copied()
                .unwrap_or(0),
        );
        let task = self
            .tasks
            .get_mut(&task_id.id())
            .ok_or_else(|| StorageError::not_found("Schema task was not found"))?;
        task.status = if status == StorageSchemaWorkStatus::Complete {
            StorageTaskStatus::Succeeded
        } else {
            StorageTaskStatus::Cancelled
        };
        task.request_payload = None;
        task.request_redacted_at = Some(Utc::now());
        task.finished_at = Some(Utc::now());
        task.updated_at = Utc::now();
        task.lease_expires_at = None;
        task.claim_token = None;
        let status = task.status;
        self.append_task_event_record(
            task_id,
            StorageTaskEventInput::new(status.as_str(), "Schema validation finished"),
        )
    }
}

#[async_trait]
impl SchemaEvolutionStorage for MemoryStorage {
    async fn get_schema_impact_boundary(
        &self,
        target: SchemaReference,
    ) -> Result<StorageSchemaImpactBoundary, StorageError> {
        let state = self.state.read().await;
        let active = state.active_schema(target.class_id())?.reference();
        let revision = state
            .schema_revisions
            .get(&(target.class_id().id(), target.revision().get()))
            .ok_or_else(|| StorageError::not_found("Schema revision was not found"))?;
        StorageSchemaImpactBoundary::try_new(
            active,
            target,
            revision.status(),
            state
                .schema_epochs
                .get(&target.class_id().id())
                .copied()
                .unwrap_or(0),
        )
        .map_err(schema_error)
    }
    async fn schema_compliance_counts(&self) -> Result<StorageComplianceCounts, StorageError> {
        let state = self.state.read().await;
        let mut counts = StorageComplianceCounts::default();
        for object in state.objects.values() {
            counts.observe(StorageSchemaEvidence::effective_status(
                state.schema_evidence.get(&object.id().id()),
                state.active_schema(object.class_id())?,
                object.revision(),
            ));
        }
        Ok(counts)
    }

    async fn list_schema_revisions(
        &self,
        query: StorageSchemaPage,
    ) -> Result<Vec<StorageSchemaRevision>, StorageError> {
        let state = self.state.read().await;
        state.active_schema(query.class_id())?;
        if query.after() == i64::MAX {
            return Ok(Vec::new());
        }
        Ok(state
            .schema_revisions
            .range(
                (query.class_id().id(), query.after().saturating_add(1))
                    ..=(query.class_id().id(), i64::MAX),
            )
            .take(query.limit())
            .map(|(_, revision)| revision.clone())
            .collect())
    }

    async fn get_schema_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassSchemaState, StorageError> {
        self.state.read().await.schema_state(class_id)
    }

    async fn stage_schema_revision(
        &self,
        request: StorageSchemaStage,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError> {
        request
            .policy()
            .ensure_limits(self.schema_limits)
            .map_err(StorageValidationError::into_request_error)?;
        let mut guard = self.state.write().await;
        let mut state = guard.schema_mutation_delta(request.class_id());
        state.check_schema_collection(request.class_id(), request.authorized_collection())?;
        let active = state.active_schema(request.class_id())?.reference();
        if let Some(existing) = state.schema_revisions.values().rev().find(|revision| {
            revision.reference().class_id() == request.class_id()
                && revision.reference().revision() >= active.revision()
                && matches!(
                    revision.status(),
                    StorageSchemaRevisionStatus::Staged | StorageSchemaRevisionStatus::Active
                )
                && revision.policy().policy() == request.policy().policy()
        }) {
            return Ok(StorageMutationOutcome::unchanged(existing.clone()));
        }
        let last = state
            .schema_revisions
            .range((request.class_id().id(), 0)..=(request.class_id().id(), i64::MAX))
            .next_back()
            .expect("class has active revision")
            .1
            .reference()
            .revision();
        let reference = SchemaReference::new(
            request.class_id(),
            last.checked_advance().map_err(schema_error)?,
        );
        let revision = StorageSchemaRevision::staged(
            reference,
            request.policy().clone(),
            request.context(),
            Utc::now(),
        );
        state.schema_revisions.insert(
            (request.class_id().id(), reference.revision().get()),
            revision.clone(),
        );
        let receipt = state.schema_event(
            request.class_id(),
            Action::Created,
            request.context(),
            json!({"schema": reference,"status":"staged"}),
        )?;
        guard.commit_schema_delta(state);
        Ok(StorageMutationOutcome::committed(revision, receipt))
    }

    async fn abandon_schema_revision(
        &self,
        target: SchemaReference,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError> {
        let mut guard = self.state.write().await;
        let mut state = guard.schema_mutation_delta(target.class_id());
        state.check_schema_collection(target.class_id(), authorized_collection)?;
        state.active_schema(target.class_id())?;
        let revision = state
            .schema_revisions
            .get_mut(&(target.class_id().id(), target.revision().get()))
            .ok_or_else(|| StorageError::not_found("Schema revision was not found"))?;
        if revision.status() == StorageSchemaRevisionStatus::Abandoned {
            return Ok(StorageMutationOutcome::unchanged(revision.clone()));
        }
        if revision.status() != StorageSchemaRevisionStatus::Staged {
            return Err(StorageError::conflict(
                "Only staged schema revisions may be abandoned",
            ));
        }
        *revision = revision
            .clone()
            .restore_lifecycle(StorageSchemaRevisionStatus::Abandoned, None, None)
            .map_err(schema_error)?;
        let revision = revision.clone();
        let receipt = state.schema_event(
            target.class_id(),
            Action::Updated,
            context,
            json!({"schema":target,"status":"abandoned"}),
        )?;
        guard.commit_schema_delta(state);
        Ok(StorageMutationOutcome::committed(revision, receipt))
    }

    async fn activate_schema_revision(
        &self,
        request: StorageSchemaActivation,
    ) -> Result<StorageMutationOutcome<StorageSchemaActivationResult>, StorageError> {
        let mut guard = self.state.write().await;
        let class_id = request.target().class_id();
        let (total, upper) = guard.schema_object_boundary(class_id);
        let mut state = guard.schema_mutation_delta(class_id);
        state.check_schema_collection(class_id, request.authorized_collection())?;
        let before = state.active_schema(class_id)?.clone();
        if before.reference().revision() != request.expected_active() {
            return Err(StorageError::conflict("Active schema revision changed"));
        }
        if before.reference() == request.target() {
            return Ok(StorageMutationOutcome::unchanged(
                StorageSchemaActivationResult::new(before, None),
            ));
        }
        let revision = state
            .schema_revisions
            .get(&(class_id.id(), request.target().revision().get()))
            .cloned()
            .ok_or_else(|| StorageError::not_found("Schema revision was not found"))?;
        if revision.status() != StorageSchemaRevisionStatus::Staged
            || revision.reference().revision() <= before.reference().revision()
        {
            return Err(StorageError::conflict(
                "Activation requires a later staged schema revision",
            ));
        }
        let nonempty = total > 0;
        let epoch = state
            .schema_epochs
            .get(&class_id.id())
            .copied()
            .unwrap_or(0);
        if request.policy() == StorageSchemaActivationPolicy::RejectIncompatible
            && nonempty
            && !request
                .proof_task()
                .and_then(|id| state.schema_work.get(&id.id()))
                .is_some_and(|work| {
                    work.proves_compatible(
                        request.target(),
                        SchemaReference::new(
                            request.target().class_id(),
                            request.expected_active(),
                        ),
                        epoch,
                    )
                })
        {
            return Err(StorageError::conflict(
                "A current, completed compatible impact analysis is required",
            ));
        }
        state.schema_revisions.insert(
            (class_id.id(), before.reference().revision().get()),
            before
                .clone()
                .restore_lifecycle(
                    StorageSchemaRevisionStatus::Retired,
                    before.activated_at(),
                    before.activation_policy(),
                )
                .map_err(schema_error)?,
        );
        state.record_schema_history(
            class_id,
            before.reference().revision().get(),
            "update",
            request.context(),
        )?;
        let active = revision
            .restore_lifecycle(
                StorageSchemaRevisionStatus::Active,
                Some(Utc::now()),
                Some(request.policy()),
            )
            .map_err(schema_error)?;
        state.schema_revisions.insert(
            (class_id.id(), request.target().revision().get()),
            active.clone(),
        );
        state
            .schema_active
            .insert(class_id.id(), request.target().revision());
        let class = state
            .classes
            .get(&class_id.id())
            .expect("class exists")
            .clone();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(class_id.id()).map_err(schema_error)?,
            class.created_at(),
            Utc::now(),
            class.revision().checked_advance().map_err(schema_error)?,
        )
        .map_err(schema_error)?;
        let updated = StorageClass::builder(
            metadata,
            class.name(),
            class.collection_id(),
            class.description(),
        )
        .schema_policy(active.policy().policy().clone())
        .build();
        state.classes.insert(class_id.id(), updated.clone());
        state.append_history(
            MemoryHistoryValue::Class(updated),
            StorageHistoryOperation::Update,
            request.context(),
        )?;
        let work = state.enqueue_schema_work_with_boundary(
            &StorageSchemaWorkRequest::new(
                request.authorized_collection(),
                request.target(),
                StorageSchemaWorkKind::Revalidation,
                request.context().clone(),
            ),
            total,
            upper,
            true,
        )?;
        let dependent_task = state.invalidate_schema_dependency(&work)?;
        let receipt = state.schema_event(class_id, Action::Updated, request.context(), json!({"before_schema":before.reference(),"dependent_rebuild_task_id":dependent_task,"schema":request.target(),"activation_policy":request.policy(),"object_effect":if active.policy().policy().validates_schema(){"pending"}else{"not_required"},"task_id":work.task_id()}))?;
        let result = StorageSchemaActivationResult::new(active, Some(work.task_id()))
            .with_dependent_rebuild(dependent_task);
        guard.commit_schema_delta(state);
        Ok(StorageMutationOutcome::committed(result, receipt))
    }

    async fn request_schema_work(
        &self,
        request: StorageSchemaWorkRequest,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError> {
        let mut guard = self.state.write().await;
        let (total, upper) = guard.schema_object_boundary(request.target().class_id());
        let mut state = guard.schema_mutation_delta(request.target().class_id());
        state.check_schema_collection(
            request.target().class_id(),
            request.authorized_collection(),
        )?;
        let active = state.active_schema(request.target().class_id())?;
        if request.kind() == StorageSchemaWorkKind::Revalidation
            && active.reference() != request.target()
        {
            return Err(StorageError::conflict(
                "Revalidation must target the active schema",
            ));
        }
        if !state.schema_revisions.contains_key(&(
            request.target().class_id().id(),
            request.target().revision().get(),
        )) {
            return Err(StorageError::not_found("Schema revision was not found"));
        }
        let work = state.enqueue_schema_work_with_boundary(&request, total, upper, true)?;
        if guard.schema_work.contains_key(&work.task_id().id()) {
            return Ok(StorageMutationOutcome::unchanged(work));
        }
        let receipt = state.schema_event(
            request.target().class_id(),
            Action::Updated,
            request.context(),
            json!({"schema":request.target(),"task_id":work.task_id(),"work_kind":request.kind()}),
        )?;
        guard.commit_schema_delta(state);
        Ok(StorageMutationOutcome::committed(work, receipt))
    }

    async fn get_schema_work(&self, task_id: TaskId) -> Result<StorageSchemaWork, StorageError> {
        self.state
            .read()
            .await
            .schema_work
            .get(&task_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Schema work was not found"))
    }

    async fn process_schema_work(
        &self,
        lease: StorageTaskLease,
        limits: StorageSchemaBatchLimits,
    ) -> Result<StorageSchemaWork, StorageError> {
        let started = Instant::now();
        let (mut work, active, baseline, snapshots, context) = {
            let state = self.state.read().await;
            let task = state
                .tasks
                .get(&lease.task_id().id())
                .filter(|task| task.status.is_active() && task.lease_matches(&lease))
                .ok_or_else(invalid_task_lease)?;
            let work = state
                .schema_work
                .get(&lease.task_id().id())
                .cloned()
                .ok_or_else(|| StorageError::not_found("Schema work was not found"))?;
            let revision = state
                .schema_revisions
                .get(&(
                    work.target().class_id().id(),
                    work.target().revision().get(),
                ))
                .cloned()
                .ok_or_else(|| StorageError::not_found("Schema revision was not found"))?;
            let baseline = work
                .impact()
                .map(|impact| {
                    state
                        .schema_revisions
                        .get(&(
                            impact.baseline().class_id().id(),
                            impact.baseline().revision().get(),
                        ))
                        .cloned()
                        .ok_or_else(|| StorageError::not_found("Impact baseline was not found"))
                })
                .transpose()?;
            let mut bytes = 0;
            let mut rows = Vec::new();
            for object in state
                .objects
                .range((
                    std::ops::Bound::Excluded(work.cursor()),
                    std::ops::Bound::Unbounded,
                ))
                .take_while(|(id, _)| **id <= work.upper_bound())
                .map(|(_, object)| object)
                .filter(|object| object.class_id() == work.target().class_id())
            {
                let size = object_size(object.data(), limits.object_bytes());
                if size <= limits.object_bytes() && bytes + size > limits.bytes() {
                    break;
                }
                let snapshot = if size > limits.object_bytes() {
                    None
                } else {
                    bytes += size;
                    Some(object.data().clone())
                };
                rows.push((object.id(), object.revision(), snapshot));
                if rows.len() == limits.rows() {
                    break;
                }
            }
            let context = EventContext::from_mutation(MutationProvenance::worker(
                task.initiator_principal_id,
                task.id,
            ));
            (work, revision, baseline, rows, context)
        };
        let results = snapshots
            .into_iter()
            .map(|(id, revision, object)| {
                (
                    id,
                    revision,
                    if work.kind() == StorageSchemaWorkKind::Impact {
                        Some(StorageSchemaInspection::new(
                            baseline.as_ref().map(|revision| revision.policy()),
                            active.policy(),
                            object.as_ref(),
                        ))
                    } else {
                        None
                    },
                    if work.kind() == StorageSchemaWorkKind::Revalidation {
                        object.as_ref().map(|data| active.policy().inspect(data))
                    } else {
                        None
                    },
                )
            })
            .collect::<Vec<_>>();
        tokio::task::yield_now().await;
        let mut guard = self.state.write().await;
        let live = guard
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        let current = guard
            .schema_work
            .get(&work.task_id().id())
            .ok_or_else(|| StorageError::not_found("Schema work was not found"))?;
        if current.cursor() != work.cursor() || current.status() != StorageSchemaWorkStatus::Running
        {
            return Err(StorageError::conflict("Schema work checkpoint changed"));
        }
        let mut state = MemoryState::new();
        state.next_event_sequence = guard.next_event_sequence;
        state.tasks.insert(live.id.id(), live.clone());
        state.schema_work.insert(work.task_id().id(), work.clone());
        let class_id = work.target().class_id().id();
        let current_active = guard.active_schema(work.target().class_id())?.clone();
        state.classes.insert(
            class_id,
            guard
                .classes
                .get(&class_id)
                .expect("active class exists")
                .clone(),
        );
        state
            .schema_active
            .insert(class_id, current_active.reference().revision());
        state.schema_epochs.insert(
            class_id,
            guard.schema_epochs.get(&class_id).copied().unwrap_or(0),
        );
        state.schema_revisions.insert(
            (class_id, current_active.reference().revision().get()),
            current_active,
        );
        let touched = results
            .iter()
            .map(|(id, _, _, _)| id.id())
            .collect::<Vec<_>>();
        for id in &touched {
            if let Some(evidence) = guard.schema_evidence.get(id) {
                state.schema_evidence.insert(*id, evidence.clone());
            }
        }

        state
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        if work.kind() == StorageSchemaWorkKind::Revalidation
            && state.active_schema(work.target().class_id())?.reference() != work.target()
        {
            state.finish_schema_task(work.task_id(), StorageSchemaWorkStatus::Superseded)?;
        } else if results.is_empty() {
            state.finish_schema_task(work.task_id(), StorageSchemaWorkStatus::Complete)?;
        } else {
            for (id, revision, inspection, status) in results {
                let status = inspection
                    .as_ref()
                    .map_or(status, StorageSchemaInspection::status);
                let object = guard
                    .objects
                    .get(&id.id())
                    .filter(|object| {
                        object.revision() == revision
                            && object.class_id() == work.target().class_id()
                    })
                    .map(ValidationObject::new);
                let stale = object.is_none();
                if let Some(object) = object {
                    if work.kind() == StorageSchemaWorkKind::Revalidation
                        && let Some(status) = status
                    {
                        state.store_schema_result(&object, &active, status, &context)?;
                    } else if status == Some(StorageComplianceStatus::Invalid) {
                        state.impact_mismatch(&object, &active, &context)?;
                    }
                }
                if let Some(inspection) = inspection {
                    work.record_impact(id, inspection, stale);
                } else {
                    work.record(id, status, stale);
                }
            }
            work.batch_committed(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            let task = state
                .tasks
                .get_mut(&lease.task_id().id())
                .expect("live task exists");
            let processed = i32::try_from(work.examined()).unwrap_or(i32::MAX);
            let failed = i32::try_from(work.invalid() + work.uninspectable())
                .unwrap_or(processed)
                .min(processed);
            task.progress = StorageTaskProgress::try_new(
                task.progress.total().max(processed),
                processed,
                processed - failed,
                failed,
            )
            .map_err(schema_error)?;
            state.schema_work.insert(work.task_id().id(), work);
        }
        let result = state
            .schema_work
            .get(&lease.task_id().id())
            .expect("schema work exists")
            .clone();
        guard
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| task.status.is_active() && task.lease_matches(&lease))
            .ok_or_else(invalid_task_lease)?;
        guard.tasks.extend(state.tasks);
        guard.schema_work.extend(state.schema_work);
        for id in touched {
            match state.schema_evidence.remove(&id) {
                Some(evidence) => {
                    guard.schema_evidence.insert(id, evidence);
                }
                None => {
                    guard.schema_evidence.remove(&id);
                }
            }
        }
        guard.next_event_sequence = state.next_event_sequence;
        guard.events.extend(state.events);
        for (id, events) in state.task_events {
            guard.task_events.entry(id).or_default().extend(events);
        }

        Ok(result)
    }

    async fn cancel_schema_work(
        &self,
        task_id: TaskId,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError> {
        let mut guard = self.state.write().await;
        let class_id = guard
            .schema_work
            .get(&task_id.id())
            .ok_or_else(|| StorageError::not_found("Schema work was not found"))?
            .target()
            .class_id();
        let mut state = guard.schema_mutation_delta(class_id);
        let work = state
            .schema_work
            .get(&task_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Schema work was not found"))?;
        state.check_schema_collection(work.target().class_id(), authorized_collection)?;
        if work.status() != StorageSchemaWorkStatus::Running {
            return Ok(StorageMutationOutcome::unchanged(work));
        }
        state.finish_schema_task(task_id, StorageSchemaWorkStatus::Cancelled)?;
        let receipt = state.schema_event(
            work.target().class_id(),
            Action::Updated,
            context,
            json!({"schema":work.target(),"task_id":task_id,"work_status":"cancelled"}),
        )?;
        let result = state
            .schema_work
            .get(&task_id.id())
            .expect("work exists")
            .clone();
        guard.commit_schema_delta(state);
        Ok(StorageMutationOutcome::committed(result, receipt))
    }

    async fn list_schema_compliance(
        &self,
        query: StorageSchemaPage,
        status: Option<StorageComplianceStatus>,
    ) -> Result<Vec<StorageObjectCompliance>, StorageError> {
        let state = self.state.read().await;
        let active = state.active_schema(query.class_id())?;
        Ok(state
            .objects
            .values()
            .filter(|object| {
                object.class_id() == query.class_id() && i64::from(object.id().id()) > query.after()
            })
            .map(|object| {
                StorageObjectCompliance::new(
                    object.id(),
                    object.revision(),
                    active,
                    state.schema_evidence.get(&object.id().id()).cloned(),
                )
            })
            .filter(|object| status.is_none_or(|status| object.status() == status))
            .take(query.limit())
            .collect())
    }
}

fn schema_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::internal(error.to_string())
}

impl MemoryState {
    fn impact_mismatch(
        &mut self,
        object: &ValidationObject,
        revision: &StorageSchemaRevision,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        let document=AuditDocument::try_new("Schema impact analysis found an incompatible object",None,None,json!({"schema":revision.reference(),"object_revision":object.revision(),"category":"schema_mismatch","source":"impact","compliance_changed":false})).map_err(schema_error)?;
        append_memory_event!(
            self,
            EntityType::ObjectValidation,
            object.id().id(),
            Some(object.name()),
            Some(object.collection_id()),
            Action::Failed,
            context,
            document,
            Some(object.revision()),
            Some(object.revision())
        )?;
        Ok(())
    }
}

impl MemoryState {
    fn invalidate_schema_dependency(
        &mut self,
        work: &StorageSchemaWork,
    ) -> Result<Option<TaskId>, StorageError> {
        let class_id = work.target().class_id();
        if !self.computed_fields.values().any(|definition| {
            definition.class_id() == class_id
                && definition.visibility() == StorageComputedFieldVisibility::Shared
        }) {
            return Ok(None);
        }
        let task_id = TaskId::new(self.next_task_id).map_err(schema_error)?;
        self.next_task_id = self
            .next_task_id
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Task identity exhausted"))?;
        let mut task = self
            .tasks
            .get(&work.task_id().id())
            .expect("validation task exists")
            .clone();
        task.id = task_id;
        task.kind = StorageTaskKind::Reindex;
        task.progress = StorageTaskProgress::try_new(0, 0, 0, 0).map_err(schema_error)?;
        task.request_payload = Some(json!({"class_id":class_id,"schema":work.target()}));
        let class = self
            .classes
            .get(&class_id.id())
            .expect("active class exists");
        let previous = self
            .computation_states
            .get(&class_id.id())
            .cloned()
            .unwrap_or(ready_computation_state(class_id, 0, class.created_at())?);
        let next = previous
            .evaluation_revision()
            .get()
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Computation revision exhausted"))?;
        let revision =
            ready_computation_state(class_id, next, previous.created_at())?.evaluation_revision();
        let state = StorageClassComputationState::try_new(
            class_id,
            revision,
            StorageComputationRebuildState::Rebuilding {
                active_task_id: task_id,
            },
            previous.created_at(),
            Utc::now(),
        )
        .map_err(schema_error)?;
        self.tasks.insert(task_id.id(), task);
        self.computation_states.insert(class_id.id(), state);
        self.computed_rebuild_tasks.insert(task_id.id(), class_id);
        self.append_task_event_record(
            task_id,
            StorageTaskEventInput::new(
                "queued",
                "Shared computation invalidated by schema activation",
            ),
        )?;
        Ok(Some(task_id))
    }
}
