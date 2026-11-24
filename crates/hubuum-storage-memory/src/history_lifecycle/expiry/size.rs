use super::*;

pub(super) fn plan(
    state: &MemoryState,
    request: HistoryExpiryAdmissionRequest,
) -> Result<HistoryExpirySizePlan, StorageError> {
    let snapshot = state.history_lifecycle_snapshot()?;
    request
        .selection(&snapshot)
        .map_err(invalid_contract_value)?;
    let scan = request.scan().clone();
    let family = scan.key().family();
    let mut census =
        HistorySizeCensus::for_family(snapshot, family).map_err(invalid_contract_value)?;
    super::super::accounting::fold_on(state, family, &mut census)?;
    let mut selection = HistoryExpirySizeSelection::new(
        request,
        census.finish_family().map_err(invalid_contract_value)?,
    )
    .map_err(invalid_contract_value)?;
    for (_, source) in sources(state, family, 0, u64::MAX) {
        selection.remaining_time().map_err(invalid_contract_value)?;
        match source {
            ExpirySource::History(row) if row.family() != family => {}
            source => consider(state, &scan, source, &mut selection)?,
        }
    }
    selection.finish().map_err(invalid_contract_value)
}

fn consider(
    state: &MemoryState,
    scan: &HistoryLifecycleScan,
    source: ExpirySource<'_>,
    selection: &mut HistoryExpirySizeSelection,
) -> Result<(), StorageError> {
    let family = scan.key().family();
    let mut metadata_bytes = source
        .identity_bytes()?
        .ok_or_else(|| StorageError::unavailable("Size selection identity exceeds its budget"))?;
    let evidence = source
        .evidence()?
        .ok_or_else(|| StorageError::unavailable("Size selection identity is unresolved"))?;
    let interval = source
        .interval(family)?
        .ok_or_else(|| StorageError::backend_failure("Missing size selection interval"))?;
    let mut candidate = HistoryExpiryCandidate::from_evidence(
        family,
        &evidence,
        interval.anchor,
        state.history_candidate_has_dependency(family, &evidence),
    )
    .map_err(invalid_contract_value)?
    .ok_or_else(|| StorageError::unavailable("Size selection ancestry is unresolved"))?;
    let visible = state.history_identity_visible(family, Some(&evidence));
    let selected = visible && scan.includes(&candidate);
    if selected && interval.anchor.is_some() {
        candidate = candidate
            .with_native_interval(interval.from, interval.until)
            .map_err(invalid_contract_value)?;
    }
    let input_bytes = source
        .encoded_bytes()?
        .unwrap_or(u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES) + 1);
    if input_bytes <= u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES)
        && selected
        && interval.anchor.is_some()
        && let Some(source) = source.entry()
        && let MemoryHistoryValue::Object(value) = &source.value
    {
        let Some(facts) = object_authorization(source, value)? else {
            // Account for the authorization budget examined before detecting overflow.
            metadata_bytes += HistoryAuthorizationBytes::MAX_BYTES as u64;
            return selection
                .observe(candidate, input_bytes, metadata_bytes, visible)
                .map_err(invalid_contract_value);
        };
        metadata_bytes += u64::from(facts.encoded_bytes().value());
        candidate = candidate
            .with_object_authorization(facts)
            .map_err(invalid_contract_value)?;
    }
    selection
        .observe(candidate, input_bytes, metadata_bytes, visible)
        .map_err(invalid_contract_value)
}

#[cfg(test)]
mod tests;
