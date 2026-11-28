use super::expiry::{ExpirySource, sources};
use super::*;

pub(super) async fn observe(
    storage: &MemoryStorage,
    policy: HistoryPolicyReference,
    actor: PrincipalIdentity,
) -> Result<HistorySizeObservation, StorageError> {
    let before = storage.capture_history_lifecycle().await?;
    let fallback = StorageExecutionPermit::new(
        None,
        ApplicationFence::new(
            before.fence().incarnation(),
            before.fence().deletion_revision().value() as i64,
        )
        .map_err(invalid_contract_value)?,
    );
    let mut state = storage.state.write_history_control(&fallback).await?;
    let snapshot = state.history_lifecycle_snapshot()?;
    let family = policy.key().family();
    let mut census =
        HistorySizeCensus::new(snapshot.clone(), &policy).map_err(invalid_contract_value)?;
    fold_on(&state, family, &mut census)?;
    let mut accounting = census.finish().map_err(invalid_contract_value)?;
    let change = snapshot
        .control()
        .observe_size(&mut accounting, actor)
        .map_err(|error| StorageError::unavailable(error.to_string()))?;
    let events = change
        .next()
        .audit_events_since(change.expected())
        .map_err(invalid_contract_value)?;
    state.append_retained_lifecycle_events(events)?;
    state.history_lifecycle = change.next().clone();
    Ok(HistorySizeObservation::new(
        accounting,
        state.history_lifecycle_snapshot()?,
    ))
}

pub(super) fn fold_on(
    state: &MemoryState,
    family: StorageIdentityHistoryKind,
    census: &mut HistorySizeCensus,
) -> Result<(), StorageError> {
    let started = std::time::Instant::now();
    let mut consider = |source: ExpirySource<'_>| -> Result<(), StorageError> {
        if started.elapsed() > std::time::Duration::from_secs(5) {
            return Err(StorageError::unavailable(
                "Complete history accounting exceeded its time budget",
            ));
        }
        let metadata_bytes = source.identity_bytes()?.ok_or_else(|| {
            StorageError::unavailable("History accounting identity exceeds its budget")
        })?;
        let evidence = source
            .evidence()
            .map_err(|_| StorageError::unavailable("History accounting has invalid identity"))?
            .ok_or_else(|| {
                StorageError::unavailable("History accounting has unresolved identity")
            })?;
        let interval = source
            .interval(family)?
            .ok_or_else(|| StorageError::backend_failure("Missing history accounting clock"))?;
        let candidate =
            HistoryExpiryCandidate::from_evidence(family, &evidence, interval.anchor, false)
                .map_err(invalid_contract_value)?;
        let bytes = source.logical_bytes()?;
        census
            .observe(candidate.as_ref(), bytes, metadata_bytes)
            .map_err(|error| StorageError::unavailable(error.to_string()))
    };
    for (_, source) in sources(state, family, 0, u64::MAX) {
        if started.elapsed() > std::time::Duration::from_secs(5) {
            return Err(StorageError::unavailable(
                "Complete history accounting exceeded its time budget",
            ));
        }
        match source {
            ExpirySource::History(row) if row.family() != family => {}
            source => consider(source)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::complete("complete")]
    #[case::unresolved("unresolved")]
    #[case::audit_rollback("audit")]
    #[tokio::test]
    async fn refresh_is_complete_and_atomic(#[case] mode: &str) {
        let storage = MemoryStorage::new();
        let actor = PrincipalIdentity::generate();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let proposal = HistoryPolicyChange::new(
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::ClassSchema,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            HistoryPolicyRevision::new(0).unwrap(),
            HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::LogicalSize {
                    upper_logical_bytes: HistoryLogicalBytes::new(1).unwrap(),
                    lower_logical_bytes: HistoryLogicalBytes::new(0).unwrap(),
                },
                minimum_age_seconds: HistoryRetentionSeconds::new(0).unwrap(),
            },
            HistoryPolicyApplication::Prospective,
        )
        .unwrap();
        let applied = storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .apply(
                        HistoryPolicyApplyRequest::new(proposal, None),
                        actor,
                        snapshot.fence(),
                        snapshot.observed_at(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        let policy =
            HistoryPolicyReference::from(&applied.control().authority().policies().records()[0]);
        let _ = storage
            .create_class(
                StorageClassCreate::builder("accounting schema", CollectionId::new(1).unwrap(), "")
                    .build(),
                &EventContext::system(),
            )
            .await
            .unwrap();
        if mode == "audit" {
            storage
                .state
                .write_control()
                .await
                .event_sequences
                .observe(i64::MAX);
        }
        if mode == "unresolved" {
            let mut state = storage.state.write_control().await;
            let mut fields = state.schema_history.last().unwrap().fields().clone();
            fields.insert("logical_identity".into(), Value::Null);
            *state.schema_history.last_mut().unwrap() =
                StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
        }
        let before = storage.capture_history_lifecycle().await.unwrap();
        let result = storage.observe_history_size(policy.clone(), actor).await;
        if mode != "complete" {
            assert!(result.is_err());
            assert_eq!(
                storage.capture_history_lifecycle().await.unwrap().control(),
                before.control()
            );
            return;
        }
        let (accounting, first) = result.unwrap().into_parts();
        assert!(accounting.pressure().reclaiming());
        assert!(accounting.transition().is_some());
        let (repeated, after) = storage
            .observe_history_size(policy, actor)
            .await
            .unwrap()
            .into_parts();
        assert!(repeated.transition().is_none());
        assert_eq!(after.control(), first.control());
        assert_eq!(after.control().authority().size_transitions().len(), 1);
        assert_eq!(
            after
                .control()
                .authority()
                .audit_events_since(before.control().authority())
                .unwrap()
                .len(),
            1
        );
    }
}
