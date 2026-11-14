use super::*;

pub(super) fn plan(
    state: &MemoryState,
    request: HistoryExpiryAdmissionRequest,
) -> Result<HistoryExpirySizePlan, StorageError> {
    let snapshot = state.history_lifecycle_snapshot()?;
    request
        .selection(&snapshot)
        .map_err(invalid_contract_value)?;
    let family = request.scan().key().family();
    let mut census =
        HistorySizeCensus::for_family(snapshot, family).map_err(invalid_contract_value)?;
    super::super::accounting::fold_on(state, family, &mut census)?;
    let mut selection = HistoryExpirySizeSelection::new(
        request,
        census.finish_family().map_err(invalid_contract_value)?,
    )
    .map_err(invalid_contract_value)?;
    for source in sources(state, family) {
        selection.remaining_time().map_err(invalid_contract_value)?;
        match source {
            ExpirySource::History(row) if row.family() != family => {}
            ExpirySource::History(row) => consider(
                state,
                family,
                &row.backup_history_row()?.1,
                Some(row),
                &mut selection,
            )?,
            ExpirySource::Backup(row) => consider(state, family, row, None, &mut selection)?,
            ExpirySource::Computed(row) => {
                let raw = serde_json::to_value(row).map_err(|_| {
                    StorageError::backend_failure("Cannot encode computed expiry metadata")
                })?;
                consider(
                    state,
                    family,
                    &StorageBackupRow::try_from_value(raw).map_err(invalid_contract_value)?,
                    None,
                    &mut selection,
                )?;
            }
        }
    }
    selection.finish().map_err(invalid_contract_value)
}

fn consider(
    state: &MemoryState,
    family: StorageIdentityHistoryKind,
    row: &StorageBackupRow,
    source: Option<&MemoryHistoryEntry>,
    selection: &mut HistoryExpirySizeSelection,
) -> Result<(), StorageError> {
    let raw = row
        .get("logical_identity")
        .filter(|value| !value.is_null())
        .ok_or_else(|| StorageError::unavailable("Size selection identity is unresolved"))?;
    let mut metadata_bytes =
        bounded_encoded_bytes(raw, HistoryLifecycleScan::MAX_INPUT_BYTES as u64)?.ok_or_else(
            || StorageError::unavailable("Size selection identity exceeds its budget"),
        )?;
    let evidence: StorageIdentityEvidence = serde_json::from_value(raw.clone())
        .map_err(|_| StorageError::backend_failure("Invalid size selection identity"))?;
    let time = |field| {
        row.get(field)
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value::<DateTime<Utc>>)
            .transpose()
            .map_err(|_| StorageError::backend_failure("Invalid size selection interval"))
    };
    let from = time(if family == StorageIdentityHistoryKind::ClassSchema {
        "occurred_at"
    } else {
        "valid_from"
    })?
    .ok_or_else(|| StorageError::backend_failure("Missing size selection interval"))?;
    let until = time("valid_to")?;
    let anchor = if family == StorageIdentityHistoryKind::ClassSchema {
        Some(HistoryRetentionAnchor::SchemaEvent(from))
    } else if row.get("operation").and_then(Value::as_str) == Some("delete") {
        Some(HistoryRetentionAnchor::Deleted(from))
    } else {
        until.map(HistoryRetentionAnchor::Closed)
    };
    let mut candidate = HistoryExpiryCandidate::from_evidence(
        family,
        &evidence,
        anchor,
        state.history_candidate_has_dependency(family, &evidence),
    )
    .map_err(invalid_contract_value)?
    .ok_or_else(|| StorageError::unavailable("Size selection ancestry is unresolved"))?;
    if anchor.is_some() {
        candidate = candidate
            .with_native_interval(from, until)
            .map_err(invalid_contract_value)?;
    }
    let input_bytes =
        bounded_encoded_bytes(row, u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES))?
            .unwrap_or(u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES) + 1);
    if input_bytes <= u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES)
        && anchor.is_some()
        && let Some(source) = source
        && let MemoryHistoryValue::Object(value) = &source.value
    {
        let facts = HistoryObjectAuthorizationFacts::capture(
            StorageObjectHistoryRecord::try_new(value.clone(), source.metadata()?)
                .map_err(invalid_contract_value)?,
        )
        .map_err(invalid_contract_value)?;
        metadata_bytes += u64::from(facts.encoded_bytes().value());
        candidate = candidate
            .with_object_authorization(facts)
            .map_err(invalid_contract_value)?;
    }
    selection
        .observe(
            candidate,
            input_bytes,
            metadata_bytes,
            state.history_identity_visible(family, Some(&evidence)),
        )
        .map_err(invalid_contract_value)
}

#[cfg(test)]
mod tests;
