use super::*;

#[async_trait]
impl RemoteTargetStorage for MemoryStorage {
    async fn get_remote_target(
        &self,
        target_id: RemoteTargetId,
    ) -> Result<StorageRemoteTarget, StorageError> {
        self.state
            .read()
            .await
            .remote_targets
            .get(&target_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Remote target {} was not found", target_id.id()))
            })
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StoragePage<StorageRemoteTarget>, StorageError> {
        let (collection_ids, options) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .remote_targets
            .values()
            .filter(|target| collection_ids.contains(&target.collection_id()))
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<StorageMutationOutcome<StorageRemoteTarget>, StorageError> {
        let (collection_id, name, definition, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        if state.remote_targets.values().any(|target| {
            let (_, candidate_collection_id, candidate_name, _) = target.clone().into_parts();
            candidate_collection_id == collection_id && candidate_name == name
        }) {
            return Err(StorageError::conflict(format!(
                "A remote target named '{name}' already exists in collection {}",
                collection_id.id()
            )));
        }
        let id = state.next_remote_target_id;
        state.next_remote_target_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory remote target id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let target = StorageRemoteTarget::new(metadata, collection_id, &name, definition);
        state.remote_targets.insert(id, target.clone());
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            id,
            Some(&name),
            Action::Created,
            &context,
            format!("Remote target '{name}' created"),
        )?;
        state.append_history(
            MemoryHistoryValue::RemoteTarget(target.clone()),
            StorageHistoryOperation::Create,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(target, receipt))
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<StorageMutationOutcome<StorageRemoteTarget>, StorageError> {
        let (target_id, patch, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .remote_targets
            .get(&target_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Remote target {} was not found", target_id.id()))
            })?;
        let (current_metadata, current_collection_id, current_name, current_definition) =
            current.into_parts();
        let (current_description, current_transport, current_policy) =
            current_definition.into_parts();
        let transport = current_transport.into_parts();
        let (current_class_id, current_subject_types, current_enabled) =
            current_policy.into_parts();
        let patch = patch.into_parts();
        let collection_id = patch.collection_id().unwrap_or(current_collection_id);
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        let name = patch.name().unwrap_or(&current_name).to_string();
        let description = patch
            .description()
            .unwrap_or(&current_description)
            .to_string();
        let class_id = patch.class_id().unwrap_or(current_class_id);
        let subject_types = patch
            .allowed_subject_types()
            .map(<[StorageRemoteTargetSubjectType]>::to_vec)
            .unwrap_or(current_subject_types);
        let enabled = patch.enabled().unwrap_or(current_enabled);
        let body_template = match patch.body_template() {
            Some(value) => value.map(ToOwned::to_owned),
            None => transport.body_template().map(ToOwned::to_owned),
        };
        let updated_transport = StorageRemoteTargetTransport::try_new(
            patch.method().unwrap_or(transport.method()),
            patch.url_template().unwrap_or(transport.url_template()),
            patch
                .headers_template()
                .cloned()
                .unwrap_or_else(|| transport.headers_template().clone()),
            body_template,
            patch
                .auth_config()
                .cloned()
                .unwrap_or_else(|| transport.auth_config().clone()),
            patch.timeout_ms().unwrap_or(transport.timeout_ms()),
        )
        .map_err(invalid_contract_value)?;
        let updated_policy = StorageRemoteTargetPolicy::try_new(class_id, subject_types, enabled)
            .map_err(invalid_contract_value)?;
        let metadata = StorageRecordMetadata::try_new(
            current_metadata.id(),
            current_metadata.created_at(),
            Utc::now(),
            current_metadata
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .map_err(invalid_contract_value)?;
        let target = StorageRemoteTarget::new(
            metadata,
            collection_id,
            &name,
            StorageRemoteTargetDefinition::new(description, updated_transport, updated_policy),
        );
        state.remote_targets.insert(target_id.id(), target.clone());
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            target_id.id(),
            Some(&name),
            Action::Updated,
            &context,
            format!("Remote target '{name}' updated"),
        )?;
        state.append_history(
            MemoryHistoryValue::RemoteTarget(target.clone()),
            StorageHistoryOperation::Update,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(target, receipt))
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (target_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(target) = state.remote_targets.remove(&target_id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let (_, _, name, _) = target.clone().into_parts();
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            target_id.id(),
            Some(&name),
            Action::Deleted,
            &context,
            format!("Remote target '{name}' deleted"),
        )?;
        state.append_history(
            MemoryHistoryValue::RemoteTarget(target),
            StorageHistoryOperation::Delete,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (target_id, task_id, subject_type, subject_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let target = state
            .remote_targets
            .get(&target_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Remote target {} was not found", target_id.id()))
            })?;
        let (_, _, name, _) = target.into_parts();
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            target_id.id(),
            Some(&name),
            Action::Invoked,
            &context,
            format!(
                "Remote target '{name}' invoked for {} {} by task {}",
                subject_type.as_str(),
                subject_id.id(),
                task_id.id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl TaskQueueStorage for MemoryStorage {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError> {
        let mut state = self.state.write().await;
        if !state.principals.contains_key(&request.submitted_by().id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                request.submitted_by().id()
            )));
        }
        let idempotency_key = request.idempotency_key().map(|key| key.as_str().to_owned());
        if let Some(key) = idempotency_key.as_deref()
            && let Some(existing) = state.tasks.values().find(|task| {
                task.submitted_by == Some(request.submitted_by())
                    && task.idempotency_key.as_deref() == Some(key)
            })
        {
            if existing.kind == request.kind()
                && existing.request_hash.as_deref() == request.request_hash()
            {
                return existing.projection();
            }
            return Err(StorageError::conflict(
                "Idempotency-Key is already in use for a different task submission",
            ));
        }
        let active_count = state
            .tasks
            .values()
            .filter(|task| {
                task.submitted_by == Some(request.submitted_by())
                    && task.kind == request.kind()
                    && !task.status.is_terminal()
            })
            .count();
        if active_count >= request.maximum_active_tasks() {
            return Err(StorageError::rate_limited(format!(
                "Too many active {} tasks for user ({active_count} >= {}); wait for queued or running tasks to finish",
                request.kind().as_str(),
                request.maximum_active_tasks()
            )));
        }
        let id = TaskId::new(state.next_task_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_task_id += 1;
        let now = Utc::now();
        let record = MemoryTaskRecord {
            id,
            kind: request.kind(),
            status: StorageTaskStatus::Queued,
            submitted_by: Some(request.submitted_by()),
            idempotency_key,
            request_hash: request.request_hash().map(ToOwned::to_owned),
            request_payload: Some(request.request_payload().clone()),
            summary: None,
            progress: StorageTaskProgress::try_new(request.total_items(), 0, 0, 0)
                .map_err(invalid_contract_value)?,
            scope_snapshot: request.scope_snapshot().clone(),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
            lease_expires_at: None,
            attempt_count: 0,
            initiator_principal_id: Some(request.submitted_by()),
            claim_token: None,
        };
        let task = record.projection()?;
        state.tasks.insert(id.id(), record);
        state.append_task_event_record(
            id,
            StorageTaskEventInput::new(StorageTaskStatus::Queued.as_str(), "Task queued"),
        )?;
        Ok(task)
    }

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError> {
        let state = self.state.read().await;
        let task = state.tasks.get(&task_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Task {} was not found", task_id.id()))
        })?;
        let owner_group_id = task.submitted_by.and_then(|principal_id| {
            state
                .service_accounts
                .get(&principal_id.id())
                .map(StorageServiceAccount::owner_group_id)
        });
        Ok(StorageTaskAccess::new(task.projection()?, owner_group_id))
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StoragePage<StorageTask>, StorageError> {
        let (submitted_by, kind, status, options) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .tasks
            .values()
            .filter(|task| submitted_by.is_none_or(|value| task.submitted_by == Some(value)))
            .filter(|task| kind.is_none_or(|value| task.kind == value))
            .filter(|task| status.is_none_or(|value| task.status == value))
            .map(MemoryTaskRecord::projection)
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn list_task_events(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageTaskEvent>, StorageError> {
        let (task_id, options) = query.into_parts();
        let state = self.state.read().await;
        if !state.tasks.contains_key(&task_id.id()) {
            return Err(StorageError::not_found(format!(
                "Task {} was not found",
                task_id.id()
            )));
        }
        page(
            state
                .task_events
                .get(&task_id.id())
                .cloned()
                .unwrap_or_default(),
            &options,
        )
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageImportTaskResult>, StorageError> {
        let (task_id, options) = query.into_parts();
        let state = self.state.read().await;
        if !state.tasks.contains_key(&task_id.id()) {
            return Err(StorageError::not_found(format!(
                "Task {} was not found",
                task_id.id()
            )));
        }
        page(
            state
                .import_task_results
                .get(&task_id.id())
                .cloned()
                .unwrap_or_default(),
            &options,
        )
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let now = Utc::now();
        task_ids
            .into_iter()
            .filter_map(|task_id| state.export_outputs.get(&task_id.id()))
            .filter(|output| output.output_expires_at() > now)
            .map(export_output_summary)
            .collect()
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let now = Utc::now();
        task_ids
            .into_iter()
            .filter_map(|task_id| state.backup_outputs.get(&task_id.id()))
            .filter(|output| output.output_expires_at() > now)
            .map(backup_output_summary)
            .collect()
    }

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.export_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(export_output_summary(
            output,
        )?))
    }

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.backup_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(backup_output_summary(
            output,
        )?))
    }

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.export_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(output.clone()))
    }

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.backup_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(output.clone()))
    }
}

#[async_trait]
impl TaskExecutionStorage for MemoryStorage {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        let mut state = self.state.write().await;
        let Some(task_id) = state
            .tasks
            .values()
            .find(|task| task.status == StorageTaskStatus::Queued)
            .map(|task| task.id)
        else {
            return Ok(None);
        };
        let now = Utc::now();
        let expires_at = now
            .checked_add_signed(Duration::milliseconds(lease_duration.milliseconds()))
            .ok_or_else(|| StorageError::invalid_input("Task lease duration is too large"))?;
        let claim_token = Uuid::new_v4().to_string();
        let task = state
            .tasks
            .get_mut(&task_id.id())
            .expect("selected task remains present");
        task.status = StorageTaskStatus::Validating;
        task.started_at = Some(now);
        task.updated_at = now;
        task.lease_expires_at = Some(expires_at);
        task.attempt_count += 1;
        task.claim_token = Some(claim_token.clone());
        let projection = task.projection()?;
        let lease = StorageTaskLease::new(task_id, StorageTaskClaimToken::new(claim_token));
        state.append_task_event_record(
            task_id,
            StorageTaskEventInput::new(
                StorageTaskStatus::Validating.as_str(),
                "Task claimed for validation",
            ),
        )?;
        StorageTaskClaim::try_new(projection, lease)
            .map(Some)
            .map_err(invalid_contract_value)
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        let Some(task) = state.tasks.get_mut(&lease.task_id().id()) else {
            return Ok(false);
        };
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Ok(false);
        }
        task.lease_expires_at = Some(
            now.checked_add_signed(Duration::milliseconds(lease_duration.milliseconds()))
                .ok_or_else(|| StorageError::invalid_input("Task lease duration is too large"))?,
        );
        task.updated_at = now;
        Ok(true)
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        if batch_size == 0 {
            return Ok(Vec::new());
        }
        let mut state = self.state.write().await;
        let now = Utc::now();
        let task_ids = state
            .tasks
            .values()
            .filter(|task| {
                task.status.is_active()
                    && task
                        .lease_expires_at
                        .is_none_or(|expires_at| expires_at <= now)
            })
            .take(batch_size)
            .map(|task| task.id)
            .collect::<Vec<_>>();
        let mut recovered = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            let task = state
                .tasks
                .get_mut(&task_id.id())
                .expect("selected task remains present");
            task.status = StorageTaskStatus::Failed;
            task.summary = Some("Task worker lease expired".to_string());
            task.finished_at = Some(now);
            task.request_payload = None;
            task.request_redacted_at = Some(now);
            task.lease_expires_at = None;
            task.claim_token = None;
            task.updated_at = now;
            recovered.push(task.projection()?);
            state.append_task_event_record(
                task_id,
                StorageTaskEventInput::new(
                    StorageTaskStatus::Failed.as_str(),
                    "Task worker lease expired",
                ),
            )?;
        }
        Ok(recovered)
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        let (lease, event) = event.into_parts();
        let mut state = self.state.write().await;
        let task = state
            .tasks
            .get(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        state.append_task_event_record(lease.task_id(), event)
    }

    async fn update_task_state(
        &self,
        update: StorageTaskActiveUpdate,
    ) -> Result<StorageTask, StorageError> {
        let (lease, status, summary, counts, started_at) = update.into_parts();
        let mut state = self.state.write().await;
        let now = Utc::now();
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        task.status = status;
        task.summary = summary;
        task.progress = StorageTaskProgress::try_new(
            task.progress.total(),
            counts.processed(),
            counts.succeeded(),
            counts.failed(),
        )
        .map_err(invalid_contract_value)?;
        task.started_at = started_at.or(task.started_at).or(Some(now));
        task.updated_at = now;
        task.projection()
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        let (expected_kind, update, event, artifact) = completion.into_parts();
        let (lease, status, summary, counts, started_at) = update.into_parts();
        let mut state = self.state.write().await;
        let stored = state
            .tasks
            .get(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if stored.kind != expected_kind {
            return Err(StorageError::invalid_input(format!(
                "Task completion kind '{}' does not match stored task kind '{}'",
                expected_kind.as_str(),
                stored.kind.as_str()
            )));
        }
        if !stored.status.is_active() || !stored.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        let now = Utc::now();
        match artifact {
            StorageTaskCompletionArtifact::None | StorageTaskCompletionArtifact::RemoteCall(_) => {}
            StorageTaskCompletionArtifact::Export(artifact) => {
                let (identity, content, report, output_expires_at, durations) =
                    artifact.into_parts();
                let (template_name, content_type) = identity.into_parts();
                let (json_output, text_output) = content.into_parts();
                let (metadata, warnings, warning_count, truncated) = report.into_parts();
                let output = StorageExportOutput::builder(
                    lease.task_id(),
                    content_type,
                    metadata,
                    warnings,
                    output_expires_at,
                    now,
                )
                .template_name(template_name)
                .output(json_output, text_output)
                .warning_state(warning_count, truncated)
                .durations(durations)
                .try_build()
                .map_err(invalid_contract_value)?;
                state.export_outputs.insert(lease.task_id().id(), output);
            }
            StorageTaskCompletionArtifact::Backup(artifact) => {
                let (document, byte_size, sha256, output_expires_at) = artifact.into_parts();
                let output = StorageBackupOutput::try_new(
                    lease.task_id(),
                    document,
                    byte_size,
                    sha256,
                    output_expires_at,
                    now,
                )
                .map_err(invalid_contract_value)?;
                state.backup_outputs.insert(lease.task_id().id(), output);
            }
        }
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .expect("validated task remains present");
        task.status = status;
        task.summary = summary;
        task.progress = StorageTaskProgress::try_new(
            task.progress.total(),
            counts.processed(),
            counts.succeeded(),
            counts.failed(),
        )
        .map_err(invalid_contract_value)?;
        task.started_at = started_at.or(task.started_at).or(Some(now));
        task.finished_at = Some(now);
        task.request_payload = None;
        task.request_redacted_at = Some(now);
        task.lease_expires_at = None;
        task.claim_token = None;
        task.updated_at = now;
        let projection = task.projection()?;
        state.append_task_event_record(lease.task_id(), event)?;
        Ok(projection)
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        let (lease, summary, event) = failure.into_parts();
        let mut state = self.state.write().await;
        let now = Utc::now();
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        let succeeded = task.progress.succeeded();
        let processed = task.progress.processed().max(1);
        task.status = StorageTaskStatus::Failed;
        task.summary = Some(summary);
        task.progress =
            StorageTaskProgress::try_new(task.progress.total(), processed, succeeded, 1)
                .map_err(invalid_contract_value)?;
        task.started_at = task.started_at.or(Some(now));
        task.finished_at = Some(now);
        task.request_payload = None;
        task.request_redacted_at = Some(now);
        task.lease_expires_at = None;
        task.claim_token = None;
        task.updated_at = now;
        let projection = task.projection()?;
        state.append_task_event_record(lease.task_id(), event)?;
        Ok(projection)
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        let mut state = self.state.write().await;
        let before = state.export_outputs.len();
        let now = Utc::now();
        state
            .export_outputs
            .retain(|_, output| output.output_expires_at() > now);
        Ok(before - state.export_outputs.len())
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        let mut state = self.state.write().await;
        let before = state.backup_outputs.len();
        let now = Utc::now();
        state
            .backup_outputs
            .retain(|_, output| output.output_expires_at() > now);
        Ok(before - state.backup_outputs.len())
    }
}

#[async_trait]
impl BackupSnapshotStorage for MemoryStorage {
    async fn capture_backup_snapshot(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        let state = self.state.read().await;
        let mut state_sections = StorageBackupStateSection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<StorageBackupStateSections>();
        state_sections.insert(
            StorageBackupStateSection::Collections,
            state
                .collections
                .values()
                .map(|collection| {
                    memory_backup_row(serde_json::json!({
                        "id": collection.id().id(),
                        "name": collection.name(),
                        "description": collection.description(),
                        "created_at": collection.created_at(),
                        "updated_at": collection.updated_at(),
                        "parent_collection_id": collection.parent_collection_id().map(CollectionId::id),
                        "revision": collection.revision().get(),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::Classes,
            state
                .classes
                .values()
                .map(|class| {
                    memory_backup_row(serde_json::json!({
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
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::Objects,
            state
                .objects
                .values()
                .map(|object| {
                    memory_backup_row(serde_json::json!({
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
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::ClassRelations,
            state
                .class_relations
                .values()
                .map(|relation| {
                    memory_backup_row(serde_json::json!({
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
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::ObjectRelations,
            state
                .object_relations
                .values()
                .map(|relation| {
                    memory_backup_row(serde_json::json!({
                        "id": relation.metadata().id().id(),
                        "from_object_id": relation.from_object_id().id(),
                        "to_object_id": relation.to_object_id().id(),
                        "class_relation_id": relation.class_relation_id().id(),
                        "created_at": relation.metadata().created_at(),
                        "updated_at": relation.metadata().updated_at(),
                        "revision": relation.metadata().revision().get(),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        let history_sections = include_history.then(|| {
            StorageBackupHistorySection::ALL
                .iter()
                .copied()
                .map(|section| (section, Vec::new()))
                .collect()
        });
        StorageBackupSnapshot::try_new(state_sections, history_sections)
            .map_err(invalid_contract_value)
    }
}

fn memory_backup_row(value: serde_json::Value) -> Result<StorageBackupRow, StorageError> {
    StorageBackupRow::try_from_value(value).map_err(invalid_contract_value)
}

#[async_trait]
impl RestoreStorage for MemoryStorage {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        let (initiator, document, artifact, capability_hash, validation_summary, expires_at) =
            request.into_parts();
        let mut state = self.state.write().await;
        let id = RestoreJobId::new(state.next_restore_job_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_restore_job_id += 1;
        let now = Utc::now();
        let timestamps = StorageRestoreTimestamps::try_new(expires_at, None, None, now, now)
            .map_err(invalid_contract_value)?;
        let summary = StorageRestoreJobSummary::try_new(
            id,
            StorageRestoreJobStatus::Validated,
            initiator,
            artifact,
            None,
            timestamps,
        )
        .map_err(invalid_contract_value)?;
        let job = StorageRestoreJob::try_new(summary, document, capability_hash)
            .map_err(invalid_contract_value)?;
        state.restore_jobs.insert(
            id.id(),
            MemoryRestoreRecord {
                job: job.clone(),
                validation_summary,
            },
        );
        Ok(job)
    }

    async fn get_restore_job(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreJob, StorageError> {
        self.state
            .read()
            .await
            .restore_jobs
            .get(&job_id.id())
            .map(|record| record.job.clone())
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })
    }

    async fn get_restore_status(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreStatus, StorageError> {
        let state = self.state.read().await;
        let record = state.restore_jobs.get(&job_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
        })?;
        let (summary, _, capability_hash) = record.job.clone().into_parts();
        StorageRestoreStatus::try_new(summary, capability_hash, record.validation_summary.clone())
            .map_err(invalid_contract_value)
    }

    async fn expire_restore_stage(&self, job_id: RestoreJobId) -> Result<bool, StorageError> {
        let mut state = self.state.write().await;
        let Some(current) = state.restore_jobs.get(&job_id.id()).cloned() else {
            return Err(StorageError::not_found(format!(
                "Restore job {} was not found",
                job_id.id()
            )));
        };
        if current.job.summary().status() != StorageRestoreJobStatus::Validated
            || current.job.summary().timestamps().expires_at() > Utc::now()
        {
            return Ok(false);
        }
        let expired = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Expired,
            None,
            None,
            None,
            true,
        )?;
        state.restore_jobs.insert(job_id.id(), expired);
        Ok(true)
    }

    async fn start_restore_draining(
        &self,
        job_id: RestoreJobId,
    ) -> Result<DateTime<Utc>, StorageError> {
        let mut state = self.state.write().await;
        if !state.maintenance_state.is_normal() {
            return Err(StorageError::conflict(
                "Another maintenance operation is already active",
            ));
        }
        let current = state
            .restore_jobs
            .get(&job_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if current.job.summary().status() != StorageRestoreJobStatus::Validated {
            return Err(StorageError::conflict(
                "Only a validated restore can be confirmed",
            ));
        }
        if current.job.summary().timestamps().expires_at() <= Utc::now() {
            return Err(StorageError::conflict("The staged restore has expired"));
        }
        let confirmed_at = Utc::now();
        let confirmed = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Confirmed,
            None,
            Some(confirmed_at),
            None,
            false,
        )?;
        state.restore_jobs.insert(job_id.id(), confirmed);
        state.maintenance_state = MaintenanceState::Draining;
        state.maintenance_restore_job_id = Some(job_id);
        state.maintenance_generation = state.maintenance_generation.saturating_add(1);
        state.restore_instances.clear();
        Ok(confirmed_at)
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        let (job_id, _document) = request.into_parts();
        let mut state = self.state.write().await;
        if state.maintenance_state != MaintenanceState::Draining
            || state.maintenance_restore_job_id != Some(job_id)
        {
            return Err(StorageError::conflict(
                "The restore job does not own draining maintenance",
            ));
        }
        let current = state
            .restore_jobs
            .get(&job_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if current.job.summary().status() != StorageRestoreJobStatus::Confirmed {
            return Err(StorageError::conflict("The restore job is not confirmed"));
        }
        let timestamp_parts = current.job.summary().timestamps().into_parts();
        let started_at = timestamp_parts
            .confirmed_at()
            .ok_or_else(|| StorageError::internal("confirmed restore timestamp is missing"))?;
        let finished_at = Utc::now();
        let succeeded = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Succeeded,
            None,
            Some(started_at),
            Some(finished_at),
            false,
        )?;
        state.restore_jobs.insert(job_id.id(), succeeded);
        state.maintenance_state = MaintenanceState::Normal;
        state.maintenance_restore_job_id = None;
        state.restore_instances.clear();
        StorageRestoreCompletion::try_new(started_at, finished_at).map_err(invalid_contract_value)
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        let (job_id, error) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .restore_jobs
            .get(&job_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if matches!(
            current.job.summary().status(),
            StorageRestoreJobStatus::Succeeded
                | StorageRestoreJobStatus::Failed
                | StorageRestoreJobStatus::Expired
        ) {
            return Err(StorageError::conflict(
                "The restore job is already terminal",
            ));
        }
        let timestamp_parts = current.job.summary().timestamps().into_parts();
        let failed = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Failed,
            Some(error),
            timestamp_parts.confirmed_at(),
            Some(Utc::now()),
            true,
        )?;
        state.restore_jobs.insert(job_id.id(), failed);
        if state.maintenance_restore_job_id == Some(job_id) {
            state.maintenance_state = MaintenanceState::Normal;
            state.maintenance_restore_job_id = None;
            state.restore_instances.clear();
        }
        Ok(())
    }

    async fn get_restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        let state = self.state.read().await;
        Ok(StorageRestoreCoordinatorSnapshot::new(
            state.maintenance_state,
            state.maintenance_restore_job_id,
            Utc::now(),
        ))
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if state.maintenance_restore_job_id.is_none() {
            state.maintenance_state = MaintenanceState::Normal;
            state.restore_instances.clear();
        }
        Ok(())
    }

    async fn resume_terminal_restore(&self, job_id: RestoreJobId) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let status = state
            .restore_jobs
            .get(&job_id.id())
            .map(|record| record.job.summary().status())
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if matches!(
            status,
            StorageRestoreJobStatus::Succeeded
                | StorageRestoreJobStatus::Failed
                | StorageRestoreJobStatus::Expired
        ) && state.maintenance_restore_job_id == Some(job_id)
        {
            state.maintenance_state = MaintenanceState::Normal;
            state.maintenance_restore_job_id = None;
            state.restore_instances.clear();
        }
        Ok(())
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        if expire_validated_jobs {
            let expired_ids = state
                .restore_jobs
                .iter()
                .filter_map(|(id, record)| {
                    (record.job.summary().status() == StorageRestoreJobStatus::Validated
                        && record.job.summary().timestamps().expires_at() <= now)
                        .then_some(*id)
                })
                .collect::<Vec<_>>();
            for id in expired_ids {
                let current = state
                    .restore_jobs
                    .get(&id)
                    .cloned()
                    .expect("restore selected for expiry exists");
                state.restore_jobs.insert(
                    id,
                    transition_restore_record(
                        &current,
                        StorageRestoreJobStatus::Expired,
                        None,
                        None,
                        None,
                        true,
                    )?,
                );
            }
        }
        let drained = if state.maintenance_state.is_normal() {
            false
        } else {
            local_work_is_idle()
        };
        let generation = state.maintenance_generation;
        state.restore_instances.insert(
            instance_id,
            MemoryRestoreInstance {
                generation,
                drained,
                heartbeat_at: now,
            },
        );
        Ok(StorageRestoreCoordinatorSnapshot::new(
            state.maintenance_state,
            state.maintenance_restore_job_id,
            now,
        ))
    }

    async fn get_restore_drain_state(
        &self,
        heartbeat_cutoff: DateTime<Utc>,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        let state = self.state.read().await;
        let instances = state
            .restore_instances
            .iter()
            .filter(|(_, instance)| {
                instance.heartbeat_at >= heartbeat_cutoff
                    && instance.generation == state.maintenance_generation
            })
            .map(|(id, instance)| {
                StorageRestoreInstance::try_new(*id, instance.generation, instance.drained)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        StorageRestoreDrainState::try_new(state.maintenance_generation, instances)
            .map_err(invalid_contract_value)
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        self.state
            .write()
            .await
            .restore_instances
            .remove(&instance_id);
        Ok(())
    }
}

#[async_trait]
impl ImportStorage for MemoryStorage {
    async fn get_import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        self.get_collection(CollectionId::new(ROOT_COLLECTION_ID).expect("root id is valid"))
            .await
    }

    async fn get_import_collection_by_id(
        &self,
        collection_id: CollectionId,
    ) -> Result<Option<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .get(&collection_id.id())
            .cloned())
    }

    async fn get_import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        let parts = key.clone().into_parts();
        let state = self.state.read().await;
        let mut candidates = state
            .collections
            .values()
            .filter(|collection| collection.name() == parts.name)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(path) = parts.path {
            candidates.retain(|collection| {
                let mut names = Vec::new();
                let mut parent = collection.parent_collection_id();
                while let Some(parent_id) = parent {
                    let Some(ancestor) = state.collections.get(&parent_id.id()) else {
                        return false;
                    };
                    names.push(ancestor.name().to_string());
                    parent = ancestor.parent_collection_id();
                }
                names.reverse();
                names == path || (collection.id().id() == ROOT_COLLECTION_ID && path.is_empty())
            });
        }
        match candidates.as_slice() {
            [] => Ok(None),
            [collection] => Ok(Some(collection.clone())),
            _ => Err(StorageError::conflict(format!(
                "Import collection key '{}' is ambiguous",
                parts.name
            ))),
        }
    }

    async fn list_import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .values()
            .filter(|collection| collection.name() == name)
            .cloned()
            .collect())
    }

    async fn get_import_collection_child_by_name(
        &self,
        parent_collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .values()
            .find(|collection| {
                collection.parent_collection_id() == Some(parent_collection_id)
                    && collection.name() == name
            })
            .cloned())
    }

    async fn get_import_class_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageClass>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .classes
            .values()
            .find(|class| class.collection_id() == collection_id && class.name() == name)
            .cloned())
    }

    async fn list_import_classes_by_names(
        &self,
        collection_id: CollectionId,
        names: &[String],
    ) -> Result<Vec<StorageClass>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .classes
            .values()
            .filter(|class| {
                class.collection_id() == collection_id
                    && names.iter().any(|name| name == class.name())
            })
            .cloned()
            .collect())
    }

    async fn get_import_object_by_name(
        &self,
        class_id: ClassId,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .objects
            .values()
            .find(|object| object.class_id() == class_id && object.name() == name)
            .cloned())
    }

    async fn list_import_objects_by_names(
        &self,
        class_id: ClassId,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .objects
            .values()
            .filter(|object| {
                object.class_id() == class_id && names.iter().any(|name| name == object.name())
            })
            .cloned()
            .collect())
    }

    async fn has_import_class_relation(
        &self,
        left_class_id: ClassId,
        right_class_id: ClassId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .class_relations
            .values()
            .any(|relation| {
                relation.from_class_id() == left_class_id
                    && relation.to_class_id() == right_class_id
            }))
    }

    async fn has_import_object_relation(
        &self,
        left_object_id: ObjectId,
        right_object_id: ObjectId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .object_relations
            .values()
            .any(|relation| {
                relation.from_object_id() == left_object_id
                    && relation.to_object_id() == right_object_id
            }))
    }

    async fn has_import_group(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        let scope_ids = state
            .identity_scopes
            .values()
            .filter(|scope| scope.name() == identity_scope)
            .map(|scope| scope.id())
            .collect::<BTreeSet<_>>();
        Ok(state.groups.values().any(|group| {
            group.name() == group_name && scope_ids.contains(&group.identity_scope_id())
        }))
    }

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        let scratch = Self {
            state: Arc::new(RwLock::new(self.state.read().await.clone())),
        };
        let mut references = BTreeMap::new();
        let mut items = Vec::new();
        let mut aborted = false;
        for item in plan.into_items() {
            let (index, operation) = item.into_parts();
            match scratch
                .apply_import_operation(operation, &mut references)
                .await
            {
                Ok(revision) => items.push(StorageImportPreflightItem::success(index, revision)),
                Err(error) => {
                    items.push(StorageImportPreflightItem::failure(index, None, error));
                    if mode.atomicity() == StorageImportAtomicity::Strict {
                        aborted = true;
                        break;
                    }
                }
            }
        }
        Ok(StorageImportPreflight::new(items, aborted))
    }

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError> {
        let scratch = Self {
            state: Arc::new(RwLock::new(self.state.read().await.clone())),
        };
        let mut references = BTreeMap::new();
        for item in plan.into_items() {
            scratch
                .apply_import_operation(item.into_parts().1, &mut references)
                .await?;
        }
        *self.state.write().await = scratch.state.read().await.clone();
        Ok(())
    }

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
        _mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        let mut references = BTreeMap::new();
        let mut items = Vec::new();
        for item in plan.into_items() {
            let (index, operation) = item.into_parts();
            match self
                .apply_import_operation(operation, &mut references)
                .await
            {
                Ok(_) => items.push(StorageImportApplyItem::success(index)),
                Err(error) => items.push(StorageImportApplyItem::failure(index, error)),
            }
        }
        Ok(StorageImportApply::new(items, false))
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        for result in &results {
            let task_id = result.clone().into_parts().0;
            let task = state.tasks.get(&task_id.id()).ok_or_else(|| {
                StorageError::not_found(format!("Task {} was not found", task_id.id()))
            })?;
            if task.kind != StorageTaskKind::Import {
                return Err(StorageError::invalid_input(format!(
                    "Task {} is not an import task",
                    task_id.id()
                )));
            }
        }
        for result in results {
            let (task_id, item_ref, entity_kind, action, identifier, outcome, error, details) =
                result.into_parts();
            let id = ImportTaskResultId::new(state.next_import_result_id)
                .map_err(|value| StorageError::internal(value.to_string()))?;
            state.next_import_result_id += 1;
            let stored = StorageImportTaskResult::builder(
                id,
                task_id,
                entity_kind,
                action,
                outcome,
                Utc::now(),
            )
            .item_ref(item_ref)
            .identifier(identifier)
            .error(error)
            .details(details)
            .build();
            state
                .import_task_results
                .entry(task_id.id())
                .or_default()
                .push(stored);
        }
        Ok(())
    }
}

#[async_trait]
impl ExportTemplateStorage for MemoryStorage {
    async fn get_export_template(
        &self,
        template_id: ExportTemplateId,
    ) -> Result<StorageExportTemplate, StorageError> {
        self.state
            .read()
            .await
            .export_templates
            .get(&template_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Export template {} was not found",
                    template_id.id()
                ))
            })
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StoragePage<StorageExportTemplate>, StorageError> {
        let (collection_ids, options) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .export_templates
            .values()
            .filter(|template| {
                let (_, collection_id, _, _) = (*template).clone().into_parts();
                collection_ids
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&collection_id))
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: CollectionId,
        exclude_template_id: Option<ExportTemplateId>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .export_templates
            .values()
            .filter(|template| {
                let (metadata, candidate_collection_id, _, _) = (*template).clone().into_parts();
                candidate_collection_id == collection_id
                    && exclude_template_id
                        .is_none_or(|excluded| metadata.id().id() != excluded.id())
            })
            .cloned()
            .collect())
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<StorageMutationOutcome<StorageExportTemplate>, StorageError> {
        let (collection_id, name, definition, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        if state.export_templates.values().any(|template| {
            let (_, candidate_collection_id, candidate_name, _) = template.clone().into_parts();
            candidate_collection_id == collection_id && candidate_name == name
        }) {
            return Err(StorageError::conflict(format!(
                "An export template named '{name}' already exists in collection {}",
                collection_id.id()
            )));
        }
        let id = state.next_export_template_id;
        state.next_export_template_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory export template id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let template = StorageExportTemplate::new(metadata, collection_id, &name, definition);
        state.export_templates.insert(id, template.clone());
        let receipt = state.append_simple_event(
            EntityType::ExportTemplate,
            id,
            Some(&name),
            Action::Created,
            &context,
            format!("Export template '{name}' created"),
        )?;
        state.append_history(
            MemoryHistoryValue::ExportTemplate(template.clone()),
            StorageHistoryOperation::Create,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(template, receipt))
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<StorageMutationOutcome<StorageExportTemplate>, StorageError> {
        let (template_id, collection_id, name, definition, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .export_templates
            .get(&template_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Export template {} was not found",
                    template_id.id()
                ))
            })?;
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        let (current_metadata, ..) = current.into_parts();
        let metadata = StorageRecordMetadata::try_new(
            current_metadata.id(),
            current_metadata.created_at(),
            Utc::now(),
            current_metadata
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .map_err(invalid_contract_value)?;
        let template = StorageExportTemplate::new(metadata, collection_id, &name, definition);
        state
            .export_templates
            .insert(template_id.id(), template.clone());
        let receipt = state.append_simple_event(
            EntityType::ExportTemplate,
            template_id.id(),
            Some(&name),
            Action::Updated,
            &context,
            format!("Export template '{name}' updated"),
        )?;
        state.append_history(
            MemoryHistoryValue::ExportTemplate(template.clone()),
            StorageHistoryOperation::Update,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(template, receipt))
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (template_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(template) = state.export_templates.remove(&template_id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let (_, _, name, _) = template.clone().into_parts();
        let receipt = state.append_simple_event(
            EntityType::ExportTemplate,
            template_id.id(),
            Some(&name),
            Action::Deleted,
            &context,
            format!("Export template '{name}' deleted"),
        )?;
        state.append_history(
            MemoryHistoryValue::ExportTemplate(template),
            StorageHistoryOperation::Delete,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}
