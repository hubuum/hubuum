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
    let mut input_bytes = 0;
    let mut metadata_bytes = 0;
    let mut consider = |row: &StorageBackupRow,
                        source: Option<&MemoryHistoryEntry>|
     -> Result<bool, StorageError> {
        let Some(bytes) = bounded_encoded_bytes(
            row,
            u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES) - input_bytes,
        )?
        else {
            return Ok(false);
        };
        input_bytes += bytes;
        let raw = row.get("logical_identity").filter(|value| !value.is_null());
        if let Some(raw) = raw {
            let Some(bytes) = bounded_encoded_bytes(
                raw,
                HistoryLifecycleScan::MAX_INPUT_BYTES as u64 - metadata_bytes,
            )?
            else {
                return Ok(false);
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
            return Ok(true);
        };
        if !request.scan().includes(&candidate) {
            return Ok(true);
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
                return Ok(true);
            }
            let facts = HistoryObjectAuthorizationFacts::capture(
                StorageObjectHistoryRecord::try_new(value.clone(), source.metadata()?)
                    .map_err(invalid_contract_value)?,
            )
            .map_err(invalid_contract_value)?;
            let bytes = u64::from(facts.encoded_bytes().value());
            if bytes > HistoryLifecycleScan::MAX_INPUT_BYTES as u64 - metadata_bytes {
                return Ok(false);
            }
            metadata_bytes += bytes;
            candidate = candidate
                .with_object_authorization(facts)
                .map_err(invalid_contract_value)?;
        }
        let size = HistoryExpiryInputBytes::new(bytes).map_err(invalid_contract_value)?;
        Ok(!matches!(
            selection
                .consider(candidate, size, &[])
                .map_err(invalid_contract_value)?,
            HistoryExpirySelectionOutcome::BudgetReached
        ))
    };
    // Bound examined source rows before family, closure, scope, identity, visibility
    // or dependency filtering. Skipping an unselectable source never replenishes this budget.
    match family {
        StorageIdentityHistoryKind::ClassSchema => {
            for row in state
                .schema_history
                .iter()
                .take(request.scan().limit() as usize)
            {
                if !consider(row, None)? {
                    break;
                }
            }
        }
        StorageIdentityHistoryKind::ComputedDefinitions => {
            for row in state
                .computed_history
                .iter()
                .take(request.scan().limit() as usize)
            {
                if bounded_encoded_bytes(row, u64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES))?
                    .is_none()
                {
                    break;
                }
                let raw = serde_json::to_value(row).map_err(|_| {
                    StorageError::backend_failure("Cannot encode computed expiry source")
                })?;
                if !consider(
                    &StorageBackupRow::try_from_value(raw).map_err(invalid_contract_value)?,
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
            for row in state
                .relation_history
                .get(&section)
                .into_iter()
                .flatten()
                .take(request.scan().limit() as usize)
            {
                if !consider(row, None)? {
                    break;
                }
            }
        }
        _ => {
            for row in state
                .history
                .iter()
                .take(request.scan().limit() as usize)
                .filter(|row| row.family() == family)
            {
                if !consider(&row.backup_history_row()?.1, Some(row))? {
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
            .unwrap();
        assert!(
            bounded.is_none(),
            "A skipped first row must still consume the scan limit"
        );
        let wider = storage
            .run_in_scope(scope, storage.admit_history_expiry(request(100)))
            .await
            .unwrap();
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
            .unwrap();
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
