use super::*;

pub(super) async fn admit(
    storage: &MemoryStorage,
    request: HistoryExpiryAdmissionRequest,
) -> Result<Option<HistoryExpiryManifest>, StorageError> {
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
    let mut state = storage.state.write_history_control(application).await?;
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
    let mut scanned = 0;
    let mut input_bytes = 0;
    let mut metadata_bytes = 0;
    let mut consider = |row: StorageBackupRow,
                        facts: Option<HistoryObjectAuthorizationFacts>|
     -> Result<bool, StorageError> {
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
            return Ok(true);
        }
        let Some(raw) = row.get("logical_identity").filter(|value| !value.is_null()) else {
            return Ok(true);
        };
        let evidence: StorageIdentityEvidence = serde_json::from_value(raw.clone())
            .map_err(|_| StorageError::backend_failure("Invalid native expiry identity"))?;
        if evidence.has_unknown_restore_source()
            || !state.history_identity_visible(family, Some(&evidence))
        {
            return Ok(true);
        }
        if family == StorageIdentityHistoryKind::ClassSchema {
            let source_class = evidence.references().get("/class_id");
            for &(class_id, revision) in state.schema_revisions.keys() {
                let current =
                    state
                        .logical_identities
                        .record_ref(StorageIdentityTarget::SchemaRevision {
                            class_id: ClassId::new(class_id).map_err(|error| {
                                StorageError::backend_failure(error.to_string())
                            })?,
                            revision: SchemaRevision::new(revision).map_err(|error| {
                                StorageError::backend_failure(error.to_string())
                            })?,
                        })?;
                if current
                    .references()
                    .get("/class_id")
                    .is_none_or(|class| Some(class) == source_class)
                {
                    return Ok(true);
                }
            }
        }
        let anchor = if family == StorageIdentityHistoryKind::ClassSchema {
            HistoryRetentionAnchor::SchemaEvent(from)
        } else if deleted {
            HistoryRetentionAnchor::Deleted(from)
        } else {
            HistoryRetentionAnchor::Closed(until.expect("closed above"))
        };
        let Some(mut candidate) =
            HistoryExpiryCandidate::from_evidence(family, &evidence, Some(anchor), false)
                .map_err(invalid_contract_value)?
        else {
            return Ok(true);
        };
        if !request.scan().includes(&candidate) {
            return Ok(true);
        }
        if scanned == request.scan().limit() {
            return Ok(false);
        }
        scanned += 1;
        let bytes = encoded_bytes(&row)?;
        input_bytes += bytes;
        metadata_bytes += encoded_bytes(raw)?
            + facts
                .as_ref()
                .map_or(0, |facts| facts.encoded_bytes().value() as u64);
        if input_bytes > u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES)
            || metadata_bytes > HistoryLifecycleScan::MAX_INPUT_BYTES as u64
        {
            return Ok(false);
        }
        candidate = candidate
            .with_native_interval(from, until)
            .map_err(invalid_contract_value)?;
        if let Some(facts) = facts {
            candidate = candidate
                .with_object_authorization(facts)
                .map_err(invalid_contract_value)?;
        }
        let size = HistoryExpiryInputBytes::new(bytes).map_err(invalid_contract_value)?;
        let mut probe = request
            .selection(&snapshot)
            .map_err(invalid_contract_value)?;
        if !matches!(
            probe
                .consider(candidate.clone(), size, &[])
                .map_err(invalid_contract_value)?,
            HistoryExpirySelectionOutcome::Selected
        ) {
            return Ok(true);
        }
        let journal = HistoryExpiryJournal::default()
            .admit(probe.finish().map_err(invalid_contract_value)?)
            .map_err(invalid_contract_value)?;
        // ponytail: at most 1000 candidates x 1000 protected copies in this
        // logical adapter; index dependency evidence if measured contention warrants it.
        if state.require_history_current_copies_safe(&journal).is_err() {
            return Ok(true);
        }
        Ok(!matches!(
            selection
                .consider(candidate, size, &[])
                .map_err(invalid_contract_value)?,
            HistoryExpirySelectionOutcome::BudgetReached
        ))
    };
    match family {
        StorageIdentityHistoryKind::ClassSchema => {
            for row in &state.schema_history {
                if !consider(row.clone(), None)? {
                    break;
                }
            }
        }
        StorageIdentityHistoryKind::ComputedDefinitions => {
            for row in &state.computed_history {
                let raw = serde_json::to_value(row).map_err(|_| {
                    StorageError::backend_failure("Cannot encode computed expiry source")
                })?;
                if !consider(
                    StorageBackupRow::try_from_value(raw).map_err(invalid_contract_value)?,
                    None,
                )? {
                    break;
                }
            }
        }
        StorageIdentityHistoryKind::ClassRelation | StorageIdentityHistoryKind::ObjectRelation => {
            let section = if family == StorageIdentityHistoryKind::ClassRelation {
                StorageBackupHistorySection::ClassRelationHistory
            } else {
                StorageBackupHistorySection::ObjectRelationHistory
            };
            for row in state.relation_history.get(&section).into_iter().flatten() {
                if !consider(row.clone(), None)? {
                    break;
                }
            }
        }
        _ => {
            for row in state.history.iter().filter(|row| row.family() == family) {
                if row.valid_to.is_none() && row.operation != StorageHistoryOperation::Delete {
                    continue;
                }
                let facts = if let MemoryHistoryValue::Object(value) = &row.value {
                    Some(
                        HistoryObjectAuthorizationFacts::capture(
                            StorageObjectHistoryRecord::try_new(value.clone(), row.metadata()?)
                                .map_err(invalid_contract_value)?,
                        )
                        .map_err(invalid_contract_value)?,
                    )
                } else {
                    None
                };
                if !consider(row.backup_history_row()?.1, facts)? {
                    break;
                }
            }
        }
    }
    if selection.is_empty() {
        return Ok(None);
    }
    let manifest = selection.finish().map_err(invalid_contract_value)?;
    let change = snapshot
        .control()
        .admit_expiry(manifest.clone(), snapshot.fence())
        .map_err(invalid_contract_value)?;
    let pending = change
        .next()
        .audit_events_since(change.expected())
        .map_err(invalid_contract_value)?;
    state.append_retained_lifecycle_events(pending)?;
    state.history_lifecycle = change.next().clone();
    Ok(Some(manifest))
}

fn encoded_bytes(value: &impl serde::Serialize) -> Result<u64, StorageError> {
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len() as u64;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, value)
        .map_err(|_| StorageError::backend_failure("Cannot measure native expiry source"))?;
    Ok(count.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[rstest::rstest]
    #[case::eligible(false)]
    #[case::held(true)]
    #[tokio::test]
    async fn native_selection_retains_only_currently_eligible_membership(#[case] held: bool) {
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
            HistoryLifecycleScan::new(key, 1).unwrap(),
            before.fence(),
        )
        .unwrap();
        let selected = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(application),
                storage.admit_history_expiry(request),
            )
            .await
            .unwrap();
        assert_eq!(selected.is_some(), !held);
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
