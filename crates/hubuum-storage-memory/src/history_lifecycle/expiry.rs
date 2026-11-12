mod recovery;
pub(super) use recovery::recover_task;

use super::*;

enum ExpirySource<'a> {
    History(&'a MemoryHistoryEntry),
    Backup(&'a StorageBackupRow),
    Computed(&'a StorageComputedFieldHistory),
}

enum ScanStep {
    Advance,
    Oversized,
    Defer,
}

pub(super) async fn admit(
    storage: &MemoryStorage,
    request: HistoryExpiryAdmissionRequest,
) -> Result<HistoryExpiryAdmissionResult, StorageError> {
    let application = current_scope(storage)?;
    let mut state = storage.state.write_history_control(&application).await?;
    admit_on(&mut state, request)
}

fn current_scope(storage: &MemoryStorage) -> Result<StorageExecutionPermit, StorageError> {
    let scope = storage.current_execution_scope();
    if scope.storage_callback_active() || scope.history_read_override().is_some() {
        return Err(StorageError::unavailable(
            "Expiry selection requires an independent control scope",
        ));
    }
    let application = scope
        .application_access_override()
        .filter(|permit| {
            permit
                .owner()
                .is_some_and(|owner| owner.history_protocol().supports_online())
        })
        .ok_or_else(|| {
            StorageError::unavailable(
                "Expiry selection requires original history-aware application admission",
            )
        })?;
    Ok(application.clone())
}

pub(super) async fn process_task(
    storage: &MemoryStorage,
    request: HistoryExpiryTaskRequest,
) -> Result<HistoryExpiryTaskCheckpoint, StorageError> {
    let lease = request.lease().clone();
    let application = current_scope(storage)?;
    let mut guard = storage.state.write_history_control(&application).await?;
    let stored = task_for_expiry(&guard, &lease)?;
    let evidence = guard
        .logical_identities
        .record(StorageIdentityTarget::Task(lease.task_id()))?;
    let checkpoint = HistoryExpiryTaskCheckpoint::from_task(&stored, &evidence)
        .map_err(invalid_contract_value)?;
    let snapshot = guard.history_lifecycle_snapshot()?;
    let mut state = guard.clone();
    let next = if checkpoint.admitted() {
        checkpoint
            .retained_manifest(&snapshot)
            .map_err(invalid_contract_value)?;
        match request.protection() {
            Some(protection) => remove_on(&mut state, &checkpoint, protection)?,
            None if stored.checkpoint().is_none() => checkpoint,
            None => return Ok(checkpoint),
        }
    } else {
        if stored.checkpoint().is_none()
            && snapshot
                .control()
                .authority()
                .expiry()
                .intents()
                .iter()
                .any(|intent| {
                    intent.manifest().run()
                        == Uuid::from_bytes(*checkpoint.task_identity().as_bytes())
                })
        {
            return Err(StorageError::unavailable(
                "Recover the original history expiry task checkpoint",
            ));
        }
        let Some(selection) = checkpoint
            .selection_request(snapshot.fence())
            .map_err(invalid_contract_value)?
        else {
            return Ok(checkpoint);
        };
        // Stage control, events, checkpoint and accounting together. Any failure
        // leaves the original claim and exact retained membership unchanged.
        request
            .require_selection_authorization(&checkpoint)
            .map_err(invalid_contract_value)?;
        let page = admit_on(&mut state, selection)?;
        checkpoint
            .record_page(&page)
            .map_err(invalid_contract_value)?
    };
    let encoded = next.encode().map_err(invalid_contract_value)?;
    task_for_expiry(&state, &lease)?;
    let intent = state
        .history_lifecycle
        .authority()
        .expiry()
        .intent(next.batch());
    let total = intent.map_or(0, |intent| intent.manifest().members().len() as i32);
    let removed = intent.map_or(0, |intent| intent.removals().len() as i32);
    let task = state
        .tasks
        .get_mut(&lease.task_id().id())
        .expect("validated task");
    task.progress =
        StorageTaskProgress::try_new(total, removed, removed, 0).map_err(invalid_contract_value)?;
    task.checkpoint = Some(encoded);
    task.status = StorageTaskStatus::Running;
    task.updated_at = Utc::now();
    state.account_task_checkpoint(lease.task_id())?;
    state.append_task_event_record(
        lease.task_id(),
        StorageTaskEventInput::new("running", "History expiry continuation committed").with_data(
            Some(serde_json::json!({
                "batch": next.batch(), "admitted": next.admitted(),
                "pass_finished": next.pass_finished(), "finished": next.finished(), "removed": removed,
            })),
        ),
    )?;
    task_for_expiry(&state, &lease)?;
    *guard = state;
    Ok(next)
}

fn remove_on(
    state: &mut MemoryState,
    checkpoint: &HistoryExpiryTaskCheckpoint,
    protection: &HistoryExposureProtection,
) -> Result<HistoryExpiryTaskCheckpoint, StorageError> {
    let snapshot = state.history_lifecycle_snapshot()?;
    let intent = checkpoint
        .protected_intent(&snapshot, protection)
        .map_err(invalid_contract_value)?;
    if intent.is_complete() {
        return checkpoint
            .complete_batch(&snapshot, protection)
            .map_err(invalid_contract_value);
    }
    if state
        .runtime_registrations
        .values()
        .any(|runtime| !runtime.history_protocol.supports_online())
        || state
            .history_publications
            .expiry_blocked(intent.manifest().fence())
    {
        return Err(StorageError::unavailable(
            "Pre-exclusion publication has not positively retired",
        ));
    }
    let member = intent
        .manifest()
        .members()
        .iter()
        .find(|member| {
            !intent
                .removals()
                .iter()
                .any(|receipt| receipt.target() == member.target())
        })
        .expect("incomplete intent has pending membership");
    let family = member.family();
    let target = member.target();
    let matches = |evidence: &StorageIdentityEvidence| {
        evidence
            .subject()
            .and_then(|subject| subject.record_identity())
            == Some(target)
    };
    enum CopyLocation {
        History(usize),
        Schema(usize),
        Computed(usize),
        Relation(StorageBackupHistorySection, usize),
    }
    let matches_row = |row: &StorageBackupRow| {
        row.get("logical_identity")
            .cloned()
            .and_then(|raw| serde_json::from_value::<StorageIdentityEvidence>(raw).ok())
            .is_some_and(|evidence| matches(&evidence))
    };
    use StorageIdentityHistoryKind as F;
    let (location, row) = match family {
        F::ClassSchema => {
            let index = state
                .schema_history
                .iter()
                .position(matches_row)
                .ok_or_else(|| StorageError::conflict("Exact history copy is missing"))?;
            (
                CopyLocation::Schema(index),
                state.schema_history[index].clone(),
            )
        }
        F::ComputedDefinitions => {
            let index = state
                .computed_history
                .iter()
                .position(|row| row.logical_identity().is_some_and(matches))
                .ok_or_else(|| StorageError::conflict("Exact history copy is missing"))?;
            let raw = serde_json::to_value(&state.computed_history[index]).map_err(|_| {
                StorageError::backend_failure("Cannot encode computed history copy")
            })?;
            (
                CopyLocation::Computed(index),
                StorageBackupRow::try_from_value(raw).map_err(invalid_contract_value)?,
            )
        }
        F::ClassRelation | F::ObjectRelation => {
            let section = if family == F::ClassRelation {
                StorageBackupHistorySection::ClassRelationHistory
            } else {
                StorageBackupHistorySection::ObjectRelationHistory
            };
            let rows = state
                .relation_history
                .get(&section)
                .ok_or_else(|| StorageError::conflict("Exact relation history is missing"))?;
            let index = rows
                .iter()
                .position(matches_row)
                .ok_or_else(|| StorageError::conflict("Exact history copy is missing"))?;
            (CopyLocation::Relation(section, index), rows[index].clone())
        }
        _ => {
            let index = state
                .history
                .iter()
                .position(|row| row.family() == family && matches(&row.logical_identity))
                .ok_or_else(|| StorageError::conflict("Exact history copy is missing"))?;
            (
                CopyLocation::History(index),
                state.history[index].backup_history_row()?.1,
            )
        }
    };
    if bounded_encoded_bytes(&row, u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES))?.is_none() {
        return Err(StorageError::input_too_large(
            "Exact history copy exceeds removal budget",
        ));
    }
    let time = |field| {
        row.get(field)
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value::<DateTime<Utc>>)
            .transpose()
            .map_err(|_| StorageError::backend_failure("Invalid native history interval"))
    };
    let from = time(if family == F::ClassSchema {
        "occurred_at"
    } else {
        "valid_from"
    })?
    .ok_or_else(|| StorageError::conflict("Native history start is missing"))?;
    let until = time("valid_to")?;
    let deleted = row.get("operation").and_then(Value::as_str) == Some("delete");
    let anchor = if family == F::ClassSchema {
        HistoryRetentionAnchor::SchemaEvent(from)
    } else if deleted {
        HistoryRetentionAnchor::Deleted(from)
    } else {
        HistoryRetentionAnchor::Closed(
            until.ok_or_else(|| StorageError::conflict("History copy is no longer closed"))?,
        )
    };
    let evidence: StorageIdentityEvidence = serde_json::from_value(
        row.get("logical_identity")
            .cloned()
            .ok_or_else(|| StorageError::conflict("Exact history identity is missing"))?,
    )
    .map_err(|_| StorageError::backend_failure("Invalid native history identity"))?;
    let candidate = HistoryExpiryCandidate::from_evidence(
        family,
        &evidence,
        Some(anchor),
        state.history_candidate_has_dependency(family, &evidence),
    )
    .map_err(invalid_contract_value)?
    .ok_or_else(|| StorageError::conflict("History removal identity is unresolved"))?
    .with_native_interval(from, until)
    .map_err(invalid_contract_value)?;
    member
        .validate_removal(
            &candidate,
            snapshot.control().authority(),
            snapshot.observed_at(),
        )
        .map_err(invalid_contract_value)?;
    match location {
        CopyLocation::History(index) => {
            state.history.remove(index);
        }
        CopyLocation::Schema(index) => {
            state.schema_history.remove(index);
        }
        CopyLocation::Computed(index) => {
            state.computed_history.remove(index);
        }
        CopyLocation::Relation(section, index) => {
            state
                .relation_history
                .get_mut(&section)
                .expect("validated history section")
                .remove(index);
        }
    }
    let deletion = HistoryExpiryExactDeletion::from_affected_rows(
        target,
        protection.checkpoint().clone(),
        Utc::now(),
        1,
    )
    .map_err(invalid_contract_value)?;
    let change = snapshot
        .control()
        .record_expiry_removal(checkpoint.batch(), deletion, snapshot.fence())
        .map_err(invalid_contract_value)?;
    state.append_retained_lifecycle_events(
        change
            .next()
            .audit_events_since(change.expected())
            .map_err(invalid_contract_value)?,
    )?;
    state.history_lifecycle = change.next().clone();
    Ok(checkpoint.clone())
}

fn task_for_expiry(
    state: &MemoryState,
    lease: &StorageTaskLease,
) -> Result<StorageTask, StorageError> {
    state.ensure_task_lease_current(lease)?;
    let task = state
        .tasks
        .get(&lease.task_id().id())
        .filter(|task| task.status.is_active() && task.lease_matches(lease))
        .ok_or_else(invalid_task_lease)?;
    if task.control.deadline().is_none() {
        return Err(StorageError::conflict(
            "History expiry requires a pinned task deadline",
        ));
    }
    if let Some(reason) = task.control.stop_reason(Utc::now()) {
        return Err(StorageError::task_stopped(reason));
    }
    task.projection()
}

fn admit_on(
    state: &mut MemoryState,
    request: HistoryExpiryAdmissionRequest,
) -> Result<HistoryExpiryAdmissionResult, StorageError> {
    if state
        .runtime_registrations
        .values()
        .any(|runtime| !runtime.history_protocol.supports_online())
    {
        return Err(StorageError::unavailable(
            "A registered producer lacks online history fencing",
        ));
    }
    let snapshot = state.history_lifecycle_snapshot()?;
    let mut selection = request.selection(&snapshot).map_err(|_| {
        StorageError::conflict("Expiry authority changed or batch already admitted")
    })?;
    let family = request.scan().key().family();
    let rows: Box<dyn ExactSizeIterator<Item = ExpirySource<'_>>> = match family {
        StorageIdentityHistoryKind::ClassSchema => {
            Box::new(state.schema_history.iter().map(ExpirySource::Backup))
        }
        StorageIdentityHistoryKind::ComputedDefinitions => {
            Box::new(state.computed_history.iter().map(ExpirySource::Computed))
        }
        StorageIdentityHistoryKind::ClassRelation | StorageIdentityHistoryKind::ObjectRelation => {
            let section = if family == StorageIdentityHistoryKind::ClassRelation {
                StorageBackupHistorySection::ClassRelationHistory
            } else {
                StorageBackupHistorySection::ObjectRelationHistory
            };
            Box::new(
                state
                    .relation_history
                    .get(&section)
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
                    .iter()
                    .map(ExpirySource::Backup),
            )
        }
        StorageIdentityHistoryKind::Collection
        | StorageIdentityHistoryKind::Class
        | StorageIdentityHistoryKind::Object
        | StorageIdentityHistoryKind::ExportTemplate
        | StorageIdentityHistoryKind::RemoteTarget => {
            Box::new(state.history.iter().map(ExpirySource::History))
        }
    };
    let mut window = request
        .scan_window(rows.len() as u64)
        .map_err(invalid_contract_value)?;
    let start = usize::try_from(window.after()).map_err(|_| {
        StorageError::invalid_input("History scan position exceeds this adapter's range")
    })?;
    let span = usize::try_from(window.through() - window.after()).map_err(|_| {
        StorageError::invalid_input("History scan boundary exceeds this adapter's range")
    })?;
    let remaining = rows.len().saturating_sub(start).min(span);
    let mut input_bytes = 0;
    let mut metadata_bytes = 0;
    let mut consider = |row: &StorageBackupRow,
                        source: Option<&MemoryHistoryEntry>|
     -> Result<ScanStep, StorageError> {
        let Some(bytes) = bounded_encoded_bytes(
            row,
            u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES) - input_bytes,
        )?
        else {
            return Ok(if input_bytes == 0 {
                ScanStep::Oversized
            } else {
                ScanStep::Defer
            });
        };
        input_bytes += bytes;
        let metadata_before_row = metadata_bytes;
        let raw = row.get("logical_identity").filter(|value| !value.is_null());
        if let Some(raw) = raw {
            let Some(bytes) = bounded_encoded_bytes(
                raw,
                HistoryLifecycleScan::MAX_INPUT_BYTES as u64 - metadata_bytes,
            )?
            else {
                return Ok(if metadata_bytes == 0 {
                    ScanStep::Oversized
                } else {
                    ScanStep::Defer
                });
            };
            metadata_bytes += bytes;
        }
        let time = |field| {
            row.get(field)
                .filter(|value| !value.is_null())
                .cloned()
                .map(serde_json::from_value::<DateTime<Utc>>)
                .transpose()
                .map_err(|_| StorageError::backend_failure("Invalid native expiry interval"))
        };
        let from = time(if family == StorageIdentityHistoryKind::ClassSchema {
            "occurred_at"
        } else {
            "valid_from"
        })?
        .ok_or_else(|| StorageError::backend_failure("Missing native expiry start"))?;
        let until = time("valid_to")?;
        let deleted = row.get("operation").and_then(Value::as_str) == Some("delete");
        if family != StorageIdentityHistoryKind::ClassSchema && until.is_none() && !deleted {
            return Ok(ScanStep::Advance);
        }
        let Some(raw) = row.get("logical_identity").filter(|value| !value.is_null()) else {
            return Ok(ScanStep::Advance);
        };
        let evidence: StorageIdentityEvidence = serde_json::from_value(raw.clone())
            .map_err(|_| StorageError::backend_failure("Invalid native expiry identity"))?;
        if evidence.has_unknown_restore_source()
            || !state.history_identity_visible(family, Some(&evidence))
        {
            return Ok(ScanStep::Advance);
        }
        let anchor = if family == StorageIdentityHistoryKind::ClassSchema {
            HistoryRetentionAnchor::SchemaEvent(from)
        } else if deleted {
            HistoryRetentionAnchor::Deleted(from)
        } else {
            HistoryRetentionAnchor::Closed(until.expect("closed above"))
        };
        let Some(mut candidate) = HistoryExpiryCandidate::from_evidence(
            family,
            &evidence,
            Some(anchor),
            state.history_candidate_has_dependency(family, &evidence),
        )
        .map_err(invalid_contract_value)?
        else {
            return Ok(ScanStep::Advance);
        };
        if !request.scan().includes(&candidate) {
            return Ok(ScanStep::Advance);
        }
        candidate = candidate
            .with_native_interval(from, until)
            .map_err(invalid_contract_value)?;
        if let Some(source) = source
            && let MemoryHistoryValue::Object(value) = &source.value
        {
            // Capture authorization only after bounded identity/scope selection.
            // Missing older facts stay unresolved instead of poisoning a batch.
            if candidate.attributions().is_empty() || evidence.has_unknown_restore_source() {
                return Ok(ScanStep::Advance);
            }
            let facts = HistoryObjectAuthorizationFacts::capture(
                StorageObjectHistoryRecord::try_new(value.clone(), source.metadata()?)
                    .map_err(invalid_contract_value)?,
            )
            .map_err(invalid_contract_value)?;
            let bytes = u64::from(facts.encoded_bytes().value());
            if bytes > HistoryLifecycleScan::MAX_INPUT_BYTES as u64 - metadata_bytes {
                return Ok(
                    if metadata_bytes - metadata_before_row + bytes
                        > HistoryLifecycleScan::MAX_INPUT_BYTES as u64
                    {
                        ScanStep::Oversized
                    } else {
                        ScanStep::Defer
                    },
                );
            }
            metadata_bytes += bytes;
            candidate = candidate
                .with_object_authorization(facts)
                .map_err(invalid_contract_value)?;
        }
        let size = HistoryExpiryInputBytes::new(bytes).map_err(invalid_contract_value)?;
        Ok(
            if matches!(
                selection
                    .consider(candidate, size, &[])
                    .map_err(invalid_contract_value)?,
                HistoryExpirySelectionOutcome::BudgetReached
            ) {
                ScanStep::Defer
            } else {
                ScanStep::Advance
            },
        )
    };
    // Bound examined source rows before family, closure, scope, identity, visibility
    // or dependency filtering. Skipping an unselectable source never replenishes this budget.
    let mut stopped = false;
    for (index, row) in rows
        .enumerate()
        .skip(start)
        .take(span)
        .take(request.scan().limit() as usize)
    {
        window
            .observe(index as u64 + 1)
            .map_err(invalid_contract_value)?;
        let step = match row {
            ExpirySource::History(row) if row.family() != family => ScanStep::Advance,
            ExpirySource::History(row) => consider(&row.backup_history_row()?.1, Some(row))?,
            ExpirySource::Backup(row) => consider(row, None)?,
            ExpirySource::Computed(row) => {
                if bounded_encoded_bytes(row, u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES))?
                    .is_none()
                {
                    ScanStep::Oversized
                } else {
                    let raw = serde_json::to_value(row).map_err(|_| {
                        StorageError::backend_failure("Cannot encode computed expiry source")
                    })?;
                    consider(
                        &StorageBackupRow::try_from_value(raw).map_err(invalid_contract_value)?,
                        None,
                    )?
                }
            }
        };
        match step {
            ScanStep::Advance => window.advance(),
            ScanStep::Oversized => {
                window.advance();
                stopped = true;
                break;
            }
            ScanStep::Defer => {
                stopped = true;
                break;
            }
        }
    }
    let exhausted = !stopped && remaining <= request.scan().limit() as usize;
    if selection.is_empty() {
        return window
            .finish(None, exhausted)
            .map_err(invalid_contract_value);
    }
    let manifest = selection.finish().map_err(invalid_contract_value)?;
    let change = snapshot
        .control()
        .admit_expiry(manifest.clone(), snapshot.fence())
        .map_err(invalid_contract_value)?;
    let result = window
        .finish(Some(manifest), exhausted)
        .map_err(invalid_contract_value)?;
    let pending = change
        .next()
        .audit_events_since(change.expected())
        .map_err(invalid_contract_value)?;
    state.append_retained_lifecycle_events(pending)?;
    state.history_lifecycle = change.next().clone();
    Ok(result)
}

fn bounded_encoded_bytes(
    value: &impl serde::Serialize,
    limit: u64,
) -> Result<Option<u64>, StorageError> {
    // Reuse the existing bounded JSON writer without allocating a payload copy.
    // No valid JSON value can fit a zero-byte remainder.
    if limit == 0 {
        return Ok(None);
    }
    let budget = StorageBackupBudget::new(limit as usize, 1).map_err(invalid_contract_value)?;
    match budget.serialize_into(value, std::io::sink()) {
        Ok(bytes) => Ok(Some(bytes as u64)),
        Err(error) if error.kind() == StorageErrorKind::InputTooLarge => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod progress_tests;
#[cfg(test)]
mod task_tests;

#[cfg(test)]
mod tests {
    use super::*;

    async fn schema_class(storage: &MemoryStorage, name: &str) -> StorageClass {
        storage
            .create_class(
                StorageClassCreate::builder(name, CollectionId::new(1).unwrap(), "").build(),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value()
    }

    async fn delete_schema_class(storage: &MemoryStorage, class: &StorageClass) {
        let resolved = storage
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .unwrap();
        storage
            .delete_class(&resolved, &EventContext::system())
            .await
            .unwrap()
            .into_value();
    }

    #[rstest::rstest]
    #[case::retained(true)]
    #[case::released(false)]
    #[tokio::test]
    async fn inspection_preserves_schema_revision_dependency(#[case] retained: bool) {
        let storage = MemoryStorage::new();
        let class = schema_class(&storage, "schema dependency").await;
        if !retained {
            delete_schema_class(&storage, &class).await;
        }
        let scan = HistoryLifecycleScan::new(
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::ClassSchema,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            1,
        )
        .unwrap();
        let observed = storage.scan_history_lifecycle(scan).await.unwrap();
        let decision = storage
            .capture_history_lifecycle()
            .await
            .unwrap()
            .control()
            .authority()
            .inspect(&observed.candidates()[0], Utc::now(), &[])
            .unwrap();
        assert_eq!(
            decision.blocker(),
            Some(if retained {
                HistoryExpiryBlocker::RetainedDependency
            } else {
                HistoryExpiryBlocker::ExpiryDisabled
            })
        );
    }

    #[rstest::rstest]
    #[case::protected("protected")]
    #[case::other_scope("other_scope")]
    #[case::missing_identity("missing_identity")]
    #[tokio::test]
    async fn admission_counts_skipped_source_rows(#[case] reason: &str) {
        let storage = MemoryStorage::new();
        let first = schema_class(&storage, "first source").await;
        let second = schema_class(&storage, "second source").await;
        let scope = if reason == "other_scope" {
            let evidence = storage
                .resolve_logical_identity(StorageIdentityTarget::Resource {
                    kind: StorageIdentityResourceKind::Class,
                    id: second.id().into(),
                })
                .await
                .unwrap();
            HistoryPolicyScope::Class {
                identity: evidence
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .resource(),
            }
        } else {
            HistoryPolicyScope::Deployment
        };
        let key = HistoryPolicyKey::new(StorageIdentityHistoryKind::ClassSchema, scope).unwrap();
        let before = storage.capture_history_lifecycle().await.unwrap();
        let change = HistoryPolicyChange::new(
            key,
            HistoryPolicyRevision::new(0).unwrap(),
            HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::Age {
                    age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                    deleted_age_seconds: None,
                },
                minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
            },
            HistoryPolicyApplication::Prospective,
        )
        .unwrap();
        storage
            .commit_history_lifecycle(
                before
                    .control()
                    .apply(
                        HistoryPolicyApplyRequest::new(change, None),
                        PrincipalIdentity::generate(),
                        before.fence(),
                        before.observed_at() - Duration::seconds(1),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        delete_schema_class(&storage, &second).await;
        if reason == "other_scope" {
            delete_schema_class(&storage, &first).await;
        } else if reason == "missing_identity" {
            // A legacy source is valid retained input, but cannot be selected.
            let mut state = storage.state.write_control().await;
            let mut fields = state.schema_history[0].fields().clone();
            fields.insert("logical_identity".into(), Value::Null);
            state.schema_history[0] =
                StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
        }
        let runtime = storage.register_history_runtime(None).await.unwrap();
        let application = storage
            .capture_application_execution(Some(runtime.owner()))
            .await
            .unwrap();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let request = |limit| {
            HistoryExpiryAdmissionRequest::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                HistoryExpiryOrigin::Manual {
                    principal: PrincipalIdentity::generate(),
                },
                HistoryLifecycleScan::new(key, limit).unwrap(),
                snapshot.fence(),
            )
            .unwrap()
        };
        let scope = StorageExecutionScope::default().with_application_access(application);
        let bounded = storage
            .run_in_scope(scope.clone(), storage.admit_history_expiry(request(1)))
            .await
            .unwrap()
            .into_manifest();
        assert!(
            bounded.is_none(),
            "A skipped first row must still consume the scan limit"
        );
        let wider = storage
            .run_in_scope(scope, storage.admit_history_expiry(request(100)))
            .await
            .unwrap()
            .into_manifest();
        assert!(
            wider.is_some(),
            "The wider scan must reach the eligible second source"
        );
    }

    #[rstest::rstest]
    #[case::exact(5, Some(5))]
    #[case::over(4, None)]
    #[test]
    fn native_input_measurement_stops_at_its_bound(
        #[case] limit: u64,
        #[case] expected: Option<u64>,
    ) {
        assert_eq!(bounded_encoded_bytes(&"abc", limit).unwrap(), expected);
    }

    #[rstest::rstest]
    #[case::eligible(false, 1000, true)]
    #[case::held(true, 1000, false)]
    #[case::open_prefix(false, 1, false)]
    #[tokio::test]
    async fn native_selection_retains_only_currently_eligible_membership(
        #[case] held: bool,
        #[case] limit: u32,
        #[case] expected: bool,
    ) {
        let storage = MemoryStorage::new();
        let principal = PrincipalIdentity::generate();
        let collection = storage
            .create_collection(
                StorageCollectionCreate::new(
                    "expiry batch",
                    "original",
                    GroupId::new(1).unwrap(),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let identity = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: collection.id().into(),
            })
            .await
            .unwrap()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
            .resource();
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Collection,
            HistoryPolicyScope::Collection { identity },
        )
        .unwrap();
        let before = storage.capture_history_lifecycle().await.unwrap();
        let change = HistoryPolicyChange::new(
            key,
            HistoryPolicyRevision::new(0).unwrap(),
            HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::Age {
                    age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
                    deleted_age_seconds: None,
                },
                minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
            },
            HistoryPolicyApplication::Prospective,
        )
        .unwrap();
        storage
            .commit_history_lifecycle(
                before
                    .control()
                    .apply(
                        HistoryPolicyApplyRequest::new(change, None),
                        principal,
                        before.fence(),
                        before.observed_at() - Duration::seconds(1),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        storage
            .update_collection(
                collection.id(),
                StorageCollectionUpdate::new(None, Some("new".into())),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        if held {
            let before = storage.capture_history_lifecycle().await.unwrap();
            storage
                .commit_history_lifecycle(
                    before
                        .control()
                        .create_hold(
                            HistoryRetentionHold::new(
                                Uuid::new_v4(),
                                key,
                                before.observed_at(),
                                None,
                                principal,
                            )
                            .unwrap(),
                            before.fence(),
                        )
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        let runtime = storage.register_history_runtime(None).await.unwrap();
        let application = storage
            .capture_application_execution(Some(runtime.owner()))
            .await
            .unwrap();
        let before = storage.capture_history_lifecycle().await.unwrap();
        let request = HistoryExpiryAdmissionRequest::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual { principal },
            HistoryLifecycleScan::new(key, limit).unwrap(),
            before.fence(),
        )
        .unwrap();
        let selected = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(application),
                storage.admit_history_expiry(request),
            )
            .await
            .unwrap()
            .into_manifest();
        assert_eq!(selected.is_some(), expected);
        let state = storage.state.read_control().await;
        if let Some(manifest) = selected {
            assert_eq!(
                state
                    .history_lifecycle
                    .authority()
                    .expiry()
                    .intent(manifest.identity())
                    .unwrap()
                    .manifest(),
                &manifest
            );
            assert_eq!(manifest.members().len(), 1);
            assert!(state.history.iter().any(|row| {
                row.logical_identity
                    .subject()
                    .and_then(StorageIdentitySubject::record_identity)
                    == Some(manifest.members()[0].target())
            }));
        } else {
            assert_eq!(state.history_lifecycle, *before.control());
        }
    }
}
