mod admission;
pub(super) use admission::submit;
mod recovery;
pub(super) use recovery::recover_task;
mod size;

use super::*;
use hubuum_storage_core::StorageValidationErrorKind;
use serde::Deserialize;
use std::borrow::Cow;

#[derive(Clone, Copy)]
pub(super) enum ExpirySource<'a> {
    History(&'a MemoryHistoryEntry),
    Backup(&'a StorageBackupRow),
    Computed(&'a StorageComputedFieldHistory),
}

pub(super) struct NativeHistoryInterval {
    from: DateTime<Utc>,
    until: Option<DateTime<Utc>>,
    pub(super) anchor: Option<HistoryRetentionAnchor>,
}

impl<'a> ExpirySource<'a> {
    fn entry(self) -> Option<&'a MemoryHistoryEntry> {
        match self {
            Self::History(entry) => Some(entry),
            _ => None,
        }
    }

    fn encoded_bytes(self) -> Result<Option<u64>, StorageError> {
        let limit = u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES);
        match self {
            Self::History(entry) => {
                let Some(resource) = entry.resource_size.encoded else {
                    return Ok(None);
                };
                // Merging two nonempty objects replaces their adjacent braces with a comma.
                Ok(
                    bounded_encoded_bytes(&entry.history_metadata(), limit - resource + 1)?
                        .map(|metadata| resource - 1 + metadata),
                )
            }
            Self::Backup(row) => bounded_encoded_bytes(row, limit),
            Self::Computed(row) => bounded_encoded_bytes(row, limit),
        }
    }

    pub(super) fn identity_bytes(self) -> Result<Option<u64>, StorageError> {
        let limit = HistoryLifecycleScan::MAX_INPUT_BYTES as u64;
        match self {
            Self::History(entry) => bounded_encoded_bytes(&entry.logical_identity, limit),
            Self::Computed(row) => row
                .logical_identity()
                .map_or(Ok(Some(0)), |value| bounded_encoded_bytes(value, limit)),
            Self::Backup(row) => row
                .get("logical_identity")
                .filter(|value| !value.is_null())
                .map_or(Ok(Some(0)), |value| bounded_encoded_bytes(value, limit)),
        }
    }

    pub(super) fn evidence(self) -> Result<Option<Cow<'a, StorageIdentityEvidence>>, StorageError> {
        match self {
            Self::History(entry) => Ok(Some(Cow::Borrowed(&entry.logical_identity))),
            Self::Computed(row) => Ok(row.logical_identity().map(Cow::Borrowed)),
            Self::Backup(row) => row
                .get("logical_identity")
                .filter(|value| !value.is_null())
                .map(|value| StorageIdentityEvidence::deserialize(value).map(Cow::Owned))
                .transpose()
                .map_err(|_| StorageError::backend_failure("Invalid native history identity")),
        }
    }

    pub(super) fn interval(
        self,
        family: StorageIdentityHistoryKind,
    ) -> Result<Option<NativeHistoryInterval>, StorageError> {
        let (from, until, deleted) = match self {
            Self::History(entry) => (
                entry.valid_from,
                entry.valid_to,
                entry.operation == StorageHistoryOperation::Delete,
            ),
            Self::Computed(row) => (row.valid_from(), row.valid_to(), false),
            Self::Backup(row) => {
                let time = |field| {
                    row.get(field)
                        .filter(|value| !value.is_null())
                        .cloned()
                        .map(serde_json::from_value::<DateTime<Utc>>)
                        .transpose()
                        .map_err(|_| {
                            StorageError::backend_failure("Invalid native history interval")
                        })
                };
                let start = if family == StorageIdentityHistoryKind::ClassSchema {
                    "occurred_at"
                } else {
                    "valid_from"
                };
                let Some(from) = time(start)? else {
                    return Ok(None);
                };
                (
                    from,
                    time("valid_to")?,
                    row.get("operation").and_then(Value::as_str) == Some("delete"),
                )
            }
        };
        let anchor = if family == StorageIdentityHistoryKind::ClassSchema {
            Some(HistoryRetentionAnchor::SchemaEvent(from))
        } else if deleted {
            Some(HistoryRetentionAnchor::Deleted(from))
        } else {
            until.map(HistoryRetentionAnchor::Closed)
        };
        Ok(Some(NativeHistoryInterval {
            from,
            until,
            anchor,
        }))
    }

    pub(super) fn logical_bytes(self) -> Result<HistoryLogicalBytes, StorageError> {
        let unmeasurable =
            || StorageError::unavailable("History row exceeds logical accounting limits");
        let result = match self {
            Self::History(entry) => {
                let resource = entry
                    .resource_size
                    .logical
                    .ok_or_else(unmeasurable)?
                    .value();
                // Typed history metadata contains only strings, integers, nulls and containers,
                // so its compact encoding is already the canonical logical encoding.
                let metadata = bounded_encoded_bytes(
                    &entry.history_metadata(),
                    i64::MAX as u64 - resource + 1,
                )?
                .ok_or_else(unmeasurable)?;
                HistoryLogicalBytes::new(resource - 1 + metadata)
            }
            Self::Backup(row) => HistoryLogicalBytes::measure_fields_json_v1(row.fields()),
            Self::Computed(row) => {
                let value = serde_json::to_value(row)
                    .map_err(|error| StorageError::backend_failure(error.to_string()))?;
                HistoryLogicalBytes::measure_json_v1(&value)
            }
        };
        result.map_err(|error| StorageError::unavailable(error.to_string()))
    }
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
    let mut oversized_source_rows = 0;
    let mut size_accounting_unavailable = false;
    let mut scanned_source_rows = 0;
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
        oversized_source_rows = page.oversized_source_rows();
        size_accounting_unavailable = page.accounting_unavailable();
        scanned_source_rows = page.scanned();
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
        StorageTaskEventInput::new(
            "running",
            if size_accounting_unavailable {
                "History expiry warning: complete size accounting unavailable; only independent age eligibility considered"
            } else if oversized_source_rows > 0 {
                "History expiry warning: source rows exceeding input or identity budgets remain deferred"
            } else {
                "History expiry continuation committed"
            },
        )
        .with_data(Some(serde_json::json!({
            "batch": next.batch(), "admitted": next.admitted(),
            "pass_finished": next.pass_finished(), "finished": next.finished(), "removed": removed,
            "oversized_source_rows": oversized_source_rows,
            "size_accounting_unavailable": size_accounting_unavailable, "scanned": scanned_source_rows,
        }))),
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
        .pending_member()
        .expect("incomplete intent has pending membership");
    remove_member_on(
        state,
        member,
        checkpoint.batch(),
        &snapshot,
        protection,
        HistoryCopyScan::Bounded,
    )?;
    if intent.removals().len() + 1 == intent.manifest().members().len() {
        checkpoint
            .request_receipt_protection()
            .map_err(invalid_contract_value)
    } else {
        Ok(checkpoint.clone())
    }
}

pub(super) fn has_exact_copy(state: &MemoryState, member: &HistoryExpiryMember) -> bool {
    find_copy(state, member).is_some()
}

enum CopyLocation {
    History(usize),
    Schema(usize),
    Computed(usize),
    Relation(StorageBackupHistorySection, usize),
}

fn find_copy(state: &MemoryState, member: &HistoryExpiryMember) -> Option<CopyLocation> {
    let family = member.family();
    let target = member.target();
    let matches = |evidence: &StorageIdentityEvidence| {
        evidence
            .subject()
            .and_then(|subject| subject.record_identity())
            == Some(target)
    };
    let matches_row = |row: &StorageBackupRow| {
        row.get("logical_identity")
            .and_then(|evidence| evidence.get("subject"))
            .and_then(|subject| StorageIdentitySubject::deserialize(subject).ok())
            .and_then(|subject| subject.record_identity())
            == Some(target)
    };
    use StorageIdentityHistoryKind as F;
    match family {
        F::ClassSchema => state
            .schema_history
            .iter()
            .position(matches_row)
            .map(CopyLocation::Schema),
        F::ComputedDefinitions => state
            .computed_history
            .iter()
            .position(|row| row.logical_identity().is_some_and(matches))
            .map(CopyLocation::Computed),
        F::ClassRelation | F::ObjectRelation => {
            let section = if family == F::ClassRelation {
                StorageBackupHistorySection::ClassRelationHistory
            } else {
                StorageBackupHistorySection::ObjectRelationHistory
            };
            state
                .relation_history
                .get(&section)?
                .iter()
                .position(matches_row)
                .map(|index| CopyLocation::Relation(section, index))
        }
        _ => state
            .history
            .iter()
            .position(|row| row.family() == family && matches(&row.logical_identity))
            .map(CopyLocation::History),
    }
}

pub(super) fn remove_member_on(
    state: &mut MemoryState,
    member: &HistoryExpiryMember,
    batch: Uuid,
    snapshot: &HistoryLifecycleSnapshot,
    protection: &HistoryExposureProtection,
    scan: HistoryCopyScan,
) -> Result<bool, StorageError> {
    let Some(location) = find_copy(state, member) else {
        if snapshot
            .control()
            .authority()
            .expiry()
            .intent(batch)
            .is_some_and(|intent| {
                intent
                    .removals()
                    .iter()
                    .any(|receipt| receipt.target() == member.target())
            })
        {
            return Ok(false);
        }
        return Err(StorageError::conflict(
            "Exact history copy is missing without its first receipt",
        ));
    };
    let source = match location {
        CopyLocation::History(index) => ExpirySource::History(&state.history[index]),
        CopyLocation::Schema(index) => ExpirySource::Backup(&state.schema_history[index]),
        CopyLocation::Relation(section, index) => {
            ExpirySource::Backup(&state.relation_history[&section][index])
        }
        CopyLocation::Computed(index) => ExpirySource::Computed(&state.computed_history[index]),
    };
    let family = member.family();
    let target = member.target();
    source.encoded_bytes()?.ok_or_else(|| {
        StorageError::input_too_large("Exact history copy exceeds removal budget")
    })?;
    let interval = source
        .interval(family)?
        .ok_or_else(|| StorageError::conflict("Native history start is missing"))?;
    let anchor = interval
        .anchor
        .ok_or_else(|| StorageError::conflict("History copy is no longer closed"))?;
    let evidence = source
        .evidence()?
        .ok_or_else(|| StorageError::conflict("Exact history identity is missing"))?;
    let candidate = HistoryExpiryCandidate::from_evidence(
        family,
        &evidence,
        Some(anchor),
        state.history_candidate_has_dependency_with_scan(family, &evidence, scan),
    )
    .map_err(invalid_contract_value)?
    .ok_or_else(|| StorageError::conflict("History removal identity is unresolved"))?
    .with_native_interval(interval.from, interval.until)
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
        .record_expiry_removal(batch, deletion, snapshot.fence())
        .map_err(invalid_contract_value)?;
    state.append_retained_lifecycle_events(
        change
            .next()
            .audit_events_since(change.expected())
            .map_err(invalid_contract_value)?,
    )?;
    state.history_lifecycle = change.next().clone();
    Ok(true)
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

/// All source slices are ordered by immutable positions at insertion or restore.
fn positioned_rows<T>(
    rows: &[T],
    after: u64,
    through: u64,
    position: impl Fn(&T) -> u64,
) -> impl ExactSizeIterator<Item = (u64, &T)> {
    let start = rows.partition_point(|row| position(row) <= after);
    let end = rows.partition_point(|row| position(row) <= through);
    rows[start..end].iter().map(move |row| (position(row), row))
}

pub(super) fn sources(
    state: &MemoryState,
    family: StorageIdentityHistoryKind,
    after: u64,
    through: u64,
) -> Box<dyn ExactSizeIterator<Item = (u64, ExpirySource<'_>)> + '_> {
    use StorageIdentityHistoryKind as Family;
    match family {
        Family::ComputedDefinitions => Box::new(
            positioned_rows(&state.computed_history, after, through, |row| {
                row.history_entry_id().id() as u64
            })
            .map(|(pos, row)| (pos, ExpirySource::Computed(row))),
        ),
        Family::ClassSchema | Family::ClassRelation | Family::ObjectRelation => {
            let (rows, key) = match family {
                Family::ClassSchema => (state.schema_history.as_slice(), "id"),
                family => {
                    let section = if family == Family::ClassRelation {
                        StorageBackupHistorySection::ClassRelationHistory
                    } else {
                        StorageBackupHistorySection::ObjectRelationHistory
                    };
                    (
                        state
                            .relation_history
                            .get(&section)
                            .map(Vec::as_slice)
                            .unwrap_or_default(),
                        "history_entry_id",
                    )
                }
            };
            Box::new(
                positioned_rows(rows, after, through, move |row| {
                    row.get(key)
                        .and_then(Value::as_u64)
                        .expect("native history ID validated at insertion or restore")
                })
                .map(|(pos, row)| (pos, ExpirySource::Backup(row))),
            )
        }
        Family::Collection
        | Family::Class
        | Family::Object
        | Family::ExportTemplate
        | Family::RemoteTarget => Box::new(
            positioned_rows(&state.history, after, through, |row| row.scan_position)
                .map(|(pos, row)| (pos, ExpirySource::History(row))),
        ),
    }
}

fn admit_on(
    state: &mut MemoryState,
    request: HistoryExpiryAdmissionRequest,
) -> Result<HistoryExpiryAdmissionResult, StorageError> {
    let needs_size = request.needs_size_selection(state.history_lifecycle.authority());
    if needs_size
        && state
            .runtime_registrations
            .values()
            .all(|runtime| runtime.history_protocol.supports_online())
    {
        // Accounting is read-only until both complete passes succeed. An
        // unavailable gauge cannot suppress independently eligible age work.
        if let Ok(plan) = size::plan(state, request.clone()) {
            let (pressure, result) = plan.into_parts();
            let mut next = pressure.next().clone();
            if let Some(manifest) = result.manifest() {
                next = next
                    .admit_expiry(manifest.clone(), manifest.fence())
                    .map_err(invalid_contract_value)?
                    .next()
                    .clone();
            }
            let events = next
                .audit_events_since(pressure.expected())
                .map_err(invalid_contract_value)?;
            state.append_retained_lifecycle_events(events)?;
            state.history_lifecycle = next;
            return Ok(result);
        }
    }
    let result = admit_age_on(state, request)?;
    Ok(if needs_size {
        result.with_accounting_unavailable()
    } else {
        result
    })
}

fn admit_age_on(
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
    let mut window = request
        .scan_window(state.history_ids.last() as u64)
        .map_err(invalid_contract_value)?;
    let rows = sources(state, family, window.after(), window.through());
    let remaining = rows.len();
    let mut input_bytes = 0;
    let mut metadata_bytes = 0;
    let mut consider = |source: ExpirySource<'_>| -> Result<ScanStep, StorageError> {
        let Some(bytes) = source.encoded_bytes()? else {
            return Ok(ScanStep::Oversized);
        };
        if bytes > u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES) - input_bytes {
            return Ok(ScanStep::Defer);
        }
        input_bytes += bytes;
        let metadata_before_row = metadata_bytes;
        let Some(identity_bytes) = source.identity_bytes()? else {
            return Ok(ScanStep::Oversized);
        };
        if identity_bytes > HistoryLifecycleScan::MAX_INPUT_BYTES as u64 - metadata_bytes {
            return Ok(ScanStep::Defer);
        }
        metadata_bytes += identity_bytes;
        let interval = source
            .interval(family)?
            .ok_or_else(|| StorageError::backend_failure("Missing native expiry start"))?;
        let Some(anchor) = interval.anchor else {
            return Ok(ScanStep::Advance);
        };
        let Some(evidence) = source.evidence()? else {
            return Ok(ScanStep::Advance);
        };
        if evidence.has_unknown_restore_source()
            || !state.history_identity_visible(family, Some(&evidence))
        {
            return Ok(ScanStep::Advance);
        }
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
            .with_native_interval(interval.from, interval.until)
            .map_err(invalid_contract_value)?;
        if let Some(source) = source.entry()
            && let MemoryHistoryValue::Object(value) = &source.value
        {
            // Capture authorization only after bounded identity/scope selection.
            // Missing older facts stay unresolved instead of poisoning a batch.
            if candidate.attributions().is_empty() || evidence.has_unknown_restore_source() {
                return Ok(ScanStep::Advance);
            }
            let Some(facts) = object_authorization(source, value)? else {
                return Ok(ScanStep::Oversized);
            };
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
    for (position, row) in rows.take(request.scan().limit() as usize) {
        window.observe(position).map_err(invalid_contract_value)?;
        let step = match row {
            ExpirySource::History(row) if row.family() != family => ScanStep::Advance,
            source => consider(source)?,
        };
        match step {
            ScanStep::Advance => window.advance(),
            ScanStep::Oversized => {
                window.advance_oversized().map_err(invalid_contract_value)?;
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

fn object_authorization(
    source: &MemoryHistoryEntry,
    object: &StorageObject,
) -> Result<Option<HistoryObjectAuthorizationFacts>, StorageError> {
    match HistoryObjectAuthorizationFacts::capture_borrowed(object, &source.interval_metadata()?) {
        Ok(facts) => Ok(Some(facts)),
        Err(error) if error.kind() == StorageValidationErrorKind::ValueTooLarge => Ok(None),
        Err(error) => Err(invalid_contract_value(error)),
    }
}

#[cfg(test)]
mod progress_tests;
#[cfg(test)]
mod task_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case(StorageIdentityHistoryKind::Collection)]
    #[case(StorageIdentityHistoryKind::Class)]
    #[case(StorageIdentityHistoryKind::Object)]
    #[case(StorageIdentityHistoryKind::ExportTemplate)]
    #[case(StorageIdentityHistoryKind::RemoteTarget)]
    #[tokio::test]
    async fn native_measurements_match_the_complete_backup_after_closure(
        #[case] family: StorageIdentityHistoryKind,
    ) {
        let storage = MemoryStorage::new();
        progress_tests::seed(&storage, family, "measurement", "é\n\"").await;
        let state = storage.state.read_control().await;
        let rows: Vec<_> = state
            .history
            .iter()
            .filter(|row| row.family() == family && row.valid_to.is_some())
            .collect();
        assert!(!rows.is_empty());
        for row in rows {
            let source = ExpirySource::History(row);
            let (_, backup) = row.backup_history_row().unwrap();
            assert_eq!(
                source.encoded_bytes().unwrap(),
                Some(serde_json::to_vec(&backup).unwrap().len() as u64)
            );
            assert_eq!(
                source.logical_bytes().unwrap(),
                HistoryLogicalBytes::measure_fields_json_v1(backup.fields()).unwrap()
            );
        }
    }

    #[test]
    fn oversized_resource_still_counts_toward_logical_pressure() {
        let mut state = MemoryState::new();
        let now = Utc::now();
        let limit = HistoryExpiryInputBytes::MAX_BATCH_BYTES;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(2).unwrap(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .unwrap();
        let collection =
            StorageCollection::try_new(metadata, "large", "x".repeat(limit as usize), None)
                .unwrap();
        state
            .append_history(
                MemoryHistoryValue::Collection(collection),
                StorageHistoryOperation::Create,
                &EventContext::system(),
            )
            .unwrap();
        let entry = state.history.last().unwrap();
        let source = ExpirySource::History(entry);
        assert!(source.encoded_bytes().unwrap().is_none());
        assert!(source.logical_bytes().unwrap().value() > u64::from(limit));
    }

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
    #[case::retained(true, false)]
    #[case::unresolved(true, true)]
    #[case::released(false, false)]
    #[tokio::test]
    async fn inspection_preserves_schema_revision_dependency(
        #[case] retained: bool,
        #[case] unresolved: bool,
    ) {
        let storage = MemoryStorage::new();
        let class = schema_class(&storage, "schema dependency").await;
        if unresolved {
            let mut state = storage.state.write_control().await;
            let revision = state.schema_revisions[&(class.id().id(), 1)].clone();
            let original = revision.logical_identity().unwrap();
            let mut references = original.references().clone();
            references.insert(
                "/class_id".into(),
                StorageCapturedIdentity::Resource {
                    family: StorageIdentityResourceKind::Class,
                    reference: IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(
                            LegacyIdentityCohort::generate(),
                            ResourceId::new(class.id().id()).unwrap(),
                        ),
                    },
                },
            );
            let evidence =
                StorageIdentityEvidence::try_new(original.subject(), references).unwrap();
            let revision = state
                .logical_identities
                .schema_revision(revision.with_logical_identity(evidence));
            state
                .schema_revisions
                .insert((class.id().id(), 1), revision);
        }
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
