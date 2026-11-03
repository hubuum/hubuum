use super::*;

#[async_trait]
impl HistoryLifecycleStorage for MemoryStorage {
    async fn register_history_runtime(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError> {
        self.state
            .write_control()
            .await
            .register_runtime_protocol(isolation, HistoryRuntimeProtocol::OnlineV1)
    }
    async fn scan_history_lifecycle(
        &self,
        scan: HistoryLifecycleScan,
    ) -> Result<HistoryLifecycleScanResult, StorageError> {
        self.state.read().await?.scan_history_policy(&scan)
    }
    async fn recover_history_lifecycle(
        &self,
        recovery: HistoryLifecycleRecovery,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write_control().await;
        if state
            .application_control
            .is_released_for(&state.deletion_control)
        {
            return Err(StorageError::conflict(
                "History authority recovery requires quarantine",
            ));
        }
        let current = state.history_lifecycle_snapshot()?;
        if current.control() != recovery.expected() || current.fence() != recovery.fence() {
            return Err(StorageError::conflict(
                "History authority changed during recovery",
            ));
        }
        let pending = recovery
            .recovered()
            .authority()
            .audit_events_since(&HistoryLifecycleAuthority::empty())
            .map_err(invalid_contract_value)?;
        state.append_retained_lifecycle_events(pending)?;
        state.history_lifecycle = recovery.recovered().clone();
        Ok(())
    }
    async fn capture_history_lifecycle(&self) -> Result<HistoryLifecycleSnapshot, StorageError> {
        let state = self.state.read_control().await;
        state.history_lifecycle_snapshot()
    }
    async fn commit_history_lifecycle(
        &self,
        change: HistoryLifecycleControlChange,
    ) -> Result<HistoryLifecycleSnapshot, StorageError> {
        let mut state = self.state.write_control().await;
        let current = state.history_lifecycle_snapshot()?;
        if current.control() != change.expected() || current.fence() != change.fence() {
            return Err(StorageError::conflict(
                "History policy authority or application incarnation changed",
            ));
        }
        let admits_expiry = change
            .next()
            .authority()
            .expiry()
            .intents()
            .iter()
            .any(|intent| {
                change
                    .expected()
                    .authority()
                    .expiry()
                    .intent(intent.identity())
                    .is_none()
            });
        if admits_expiry
            && state
                .runtime_registrations
                .values()
                .any(|runtime| !runtime.history_protocol.supports_online())
        {
            return Err(StorageError::unavailable(
                "A registered producer lacks online history fencing",
            ));
        }
        for scope in change.scopes_to_validate() {
            if !state.history_policy_scope_known(scope)? {
                return Err(StorageError::not_found(
                    "History policy scope was not found",
                ));
            }
        }
        let pending = change
            .next()
            .audit_events_since(change.expected())
            .map_err(invalid_contract_value)?;
        state.append_retained_lifecycle_events(pending)?;
        state.history_lifecycle = change.next().clone();
        state.history_lifecycle_snapshot()
    }
}
impl MemoryState {
    pub(super) fn register_runtime_protocol(
        &mut self,
        isolation: Option<RuntimeIsolationDomain>,
        history_protocol: HistoryRuntimeProtocol,
    ) -> Result<RuntimeRegistration, StorageError> {
        self.ensure_task_execution_allowed()?;
        if self.runtime_registrations.len() >= DELETION_CONTROL_MAX_RUNTIMES {
            return Err(StorageError::unavailable(
                "Runtime registration capacity is exhausted; retain existing registrations",
            ));
        }
        if !history_protocol.supports_online()
            && !self.history_lifecycle.authority().expiry().is_empty()
        {
            return Err(StorageError::unavailable(
                "Retained expiry requires history-aware producers; roll forward",
            ));
        }
        let registration = RuntimeRegistration::generate();
        self.runtime_registrations.insert(
            registration.id(),
            MemoryRuntimeRegistration {
                history_protocol,
                ownership_token: registration.ownership_token(),
                isolation,
                drained: None,
            },
        );
        Ok(registration)
    }
    pub(super) fn history_identity_visible(
        &self,
        family: StorageIdentityHistoryKind,
        evidence: Option<&StorageIdentityEvidence>,
    ) -> bool {
        !self
            .history_lifecycle
            .authority()
            .expiry()
            .excludes_evidence(family, evidence)
    }
    pub(super) fn raw_history_visible(
        &self,
        family: StorageIdentityHistoryKind,
        row: &StorageBackupRow,
    ) -> Result<bool, StorageError> {
        let evidence = row
            .get("logical_identity")
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value::<StorageIdentityEvidence>)
            .transpose()
            .map_err(|_| StorageError::backend_failure("Invalid captured history identity"))?;
        Ok(self.history_identity_visible(family, evidence.as_ref()))
    }
    pub(super) fn history_entry_visible(&self, entry: &MemoryHistoryEntry) -> bool {
        self.history_identity_visible(entry.family(), Some(&entry.logical_identity))
    }
    pub(super) fn require_history_as_of(
        &self,
        family: StorageIdentityHistoryKind,
        evidence: Option<&StorageIdentityEvidence>,
        at: DateTime<Utc>,
        selected_from: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let journal = self.history_lifecycle.authority().expiry();
        if journal.excludes_evidence(family, evidence) {
            return Err(StorageError::not_found("Historical version is expired"));
        }
        if let Some(target) = evidence
            .and_then(StorageIdentityEvidence::subject)
            .and_then(StorageIdentitySubject::record_identity)
            && journal
                .restriction_as_of(family, target.resource(), at, Some(selected_from))
                .is_some()
        {
            return Err(StorageError::not_found("Historical version is expired"));
        }
        Ok(())
    }
    fn history_lifecycle_snapshot(&self) -> Result<HistoryLifecycleSnapshot, StorageError> {
        HistoryLifecycleSnapshot::new(
            self.history_lifecycle.clone(),
            self.application_control.fence().incarnation(),
            HistoryDeletionRevision::new(self.deletion_control.revision() as u64)
                .map_err(invalid_contract_value)?,
            Utc::now(),
        )
        .map_err(invalid_contract_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_domain::PrincipalIdentity;

    fn proposal() -> HistoryPolicyChange {
        HistoryPolicyChange::new(
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Object,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            HistoryPolicyRevision::new(0).unwrap(),
            HistoryPolicyBinding::Indefinite,
            HistoryPolicyApplication::Retrospective {
                closed_through: Utc::now() - Duration::seconds(1),
            },
        )
        .unwrap()
    }
    async fn issue(
        storage: &MemoryStorage,
        principal: PrincipalIdentity,
        proposal: HistoryPolicyChange,
    ) -> HistoryPolicyPreview {
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let preview = HistoryPolicyPreview::issue(
            principal,
            proposal,
            snapshot.fence(),
            Utc::now(),
            HistoryPreviewImpact::new(HistoryPreviewCoverage::Bounded, 0, 0, Vec::new()).unwrap(),
        )
        .unwrap();
        storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .issue_preview(preview.clone(), snapshot.fence())
                    .unwrap(),
            )
            .await
            .unwrap();
        preview
    }
    #[tokio::test]
    async fn empty_source_reports_complete_bounded_inspection() {
        let storage = MemoryStorage::new();
        let scan = HistoryLifecycleScan::new(
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Object,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            1,
        )
        .unwrap();
        let result = storage.scan_history_lifecycle(scan).await.unwrap();
        assert!(result.complete());
        assert_eq!(result.scanned(), 0);
        assert!(result.candidates().is_empty());
    }

    #[tokio::test]
    async fn released_application_rejects_history_authority_recovery() {
        let storage = MemoryStorage::new();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let authority = snapshot.control().authority().clone();
        let recovery = HistoryLifecycleRecovery::new(snapshot, authority).unwrap();
        let error = storage
            .recover_history_lifecycle(recovery)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Conflict);
    }

    #[tokio::test]
    async fn issued_review_applies_exact_policy_once() {
        let storage = MemoryStorage::new();
        let principal = PrincipalIdentity::generate();
        let preview = issue(&storage, principal, proposal()).await;
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let change = snapshot
            .control()
            .apply(
                HistoryPolicyApplyRequest::new(
                    preview.proposal().clone(),
                    Some(preview.reference().clone()),
                ),
                principal,
                snapshot.fence(),
                Utc::now(),
            )
            .unwrap();
        let applied = storage
            .commit_history_lifecycle(change.clone())
            .await
            .unwrap();
        assert_eq!(applied.control().authority().policies().records().len(), 1);
        assert!(storage.commit_history_lifecycle(change).await.is_err());
    }
    #[tokio::test]
    async fn concurrent_control_writers_cannot_overwrite_each_other() {
        let storage = MemoryStorage::new();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let principal = PrincipalIdentity::generate();
        let first = HistoryRetentionHold::new(
            Uuid::new_v4(),
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Object,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            Utc::now(),
            None,
            principal,
        )
        .unwrap();
        let second =
            HistoryRetentionHold::new(Uuid::new_v4(), first.key(), Utc::now(), None, principal)
                .unwrap();
        storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .create_hold(first, snapshot.fence())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            storage
                .commit_history_lifecycle(
                    snapshot
                        .control()
                        .create_hold(second, snapshot.fence())
                        .unwrap()
                )
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn creating_hold_invalidates_previously_issued_review() {
        let storage = MemoryStorage::new();
        let principal = PrincipalIdentity::generate();
        let preview = issue(&storage, principal, proposal()).await;
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let hold = HistoryRetentionHold::new(
            Uuid::new_v4(),
            preview.proposal().key(),
            Utc::now(),
            None,
            principal,
        )
        .unwrap();
        let snapshot = storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .create_hold(hold, snapshot.fence())
                    .unwrap(),
            )
            .await
            .unwrap();
        let result = snapshot.control().apply(
            HistoryPolicyApplyRequest::new(
                preview.proposal().clone(),
                Some(preview.reference().clone()),
            ),
            principal,
            snapshot.fence(),
            Utc::now(),
        );
        assert!(matches!(
            result,
            Err(HistoryPolicyApplyError::Conflict(
                HistoryPolicyReviewConflict::StalePreview
            ))
        ));
    }
    #[tokio::test]
    async fn review_is_not_an_authorization_grant_to_another_principal() {
        let storage = MemoryStorage::new();
        let preview = issue(&storage, PrincipalIdentity::generate(), proposal()).await;
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let result = snapshot.control().apply(
            HistoryPolicyApplyRequest::new(
                preview.proposal().clone(),
                Some(preview.reference().clone()),
            ),
            PrincipalIdentity::generate(),
            snapshot.fence(),
            Utc::now(),
        );
        assert!(matches!(
            result,
            Err(HistoryPolicyApplyError::Conflict(
                HistoryPolicyReviewConflict::InaccessiblePreview
            ))
        ));
    }
    #[tokio::test]
    async fn inactive_history_keeps_existing_checkpoint_components_optional() {
        let storage = MemoryStorage::new();
        let capture = storage.capture_control_checkpoint().await.unwrap();
        assert!(capture.history_lifecycle().is_none());
    }

    #[tokio::test]
    async fn authoritative_checkpoint_capture_contains_policy_and_hold_state() {
        let storage = MemoryStorage::new();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let hold = HistoryRetentionHold::new(
            Uuid::new_v4(),
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Object,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            Utc::now(),
            None,
            PrincipalIdentity::generate(),
        )
        .unwrap();
        let snapshot = storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .create_hold(hold, snapshot.fence())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            storage
                .capture_control_checkpoint()
                .await
                .unwrap()
                .history_lifecycle(),
            Some(snapshot.control().authority())
        );
    }
}

impl MemoryState {
    fn scan_history_policy(
        &self,
        scan: &HistoryLifecycleScan,
    ) -> Result<HistoryLifecycleScanResult, StorageError> {
        let family = scan.key().family();
        let mut scanned = 0;
        let mut unresolved = 0;
        let mut input_bytes = 0;
        let mut candidates = Vec::new();
        let mut exhausted = false;
        let mut consider =
            |evidence: Option<&StorageIdentityEvidence>, anchor| -> Result<bool, StorageError> {
                if scanned == scan.limit() {
                    exhausted = true;
                    return Ok(false);
                }
                if let Some(evidence) = evidence {
                    input_bytes += serde_json::to_vec(evidence)
                        .map_err(|_| StorageError::internal("Cannot size historical identity"))?
                        .len();
                    if input_bytes > HistoryLifecycleScan::MAX_INPUT_BYTES {
                        exhausted = true;
                        return Ok(false);
                    }
                    match HistoryExpiryCandidate::from_evidence(family, evidence, anchor, false)
                        .map_err(invalid_contract_value)?
                    {
                        Some(candidate) if scan.includes(&candidate) => candidates.push(candidate),
                        Some(_) => {}
                        None => unresolved += 1,
                    }
                } else {
                    unresolved += 1;
                }
                scanned += 1;
                Ok(true)
            };
        match family {
            StorageIdentityHistoryKind::ClassSchema => {
                for row in &self.schema_history {
                    let evidence = row
                        .get("logical_identity")
                        .cloned()
                        .map(serde_json::from_value::<StorageIdentityEvidence>)
                        .transpose()
                        .map_err(|_| {
                            StorageError::backend_failure("Invalid schema history identity")
                        })?;
                    let anchor = row
                        .get("occurred_at")
                        .cloned()
                        .map(serde_json::from_value::<DateTime<Utc>>)
                        .transpose()
                        .map_err(|_| {
                            StorageError::backend_failure("Invalid schema history anchor")
                        })?
                        .map(HistoryRetentionAnchor::SchemaEvent);
                    if !consider(evidence.as_ref(), anchor)? {
                        break;
                    }
                }
            }
            StorageIdentityHistoryKind::ComputedDefinitions => {
                for row in &self.computed_history {
                    if !consider(
                        row.logical_identity(),
                        row.valid_to().map(HistoryRetentionAnchor::Closed),
                    )? {
                        break;
                    }
                }
            }
            StorageIdentityHistoryKind::ClassRelation
            | StorageIdentityHistoryKind::ObjectRelation => {
                let section = if family == StorageIdentityHistoryKind::ClassRelation {
                    StorageBackupHistorySection::ClassRelationHistory
                } else {
                    StorageBackupHistorySection::ObjectRelationHistory
                };
                for row in self.relation_history.get(&section).into_iter().flatten() {
                    let evidence = row
                        .get("logical_identity")
                        .cloned()
                        .map(serde_json::from_value::<StorageIdentityEvidence>)
                        .transpose()
                        .map_err(|_| {
                            StorageError::backend_failure("Invalid relation history identity")
                        })?;
                    let deleted = row
                        .get("operation")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|value| value.eq_ignore_ascii_case("delete"));
                    let anchor = row
                        .get(if deleted { "valid_from" } else { "valid_to" })
                        .filter(|value| !value.is_null())
                        .cloned()
                        .map(serde_json::from_value::<DateTime<Utc>>)
                        .transpose()
                        .map_err(|_| {
                            StorageError::backend_failure("Invalid relation history anchor")
                        })?
                        .map(|at| {
                            if deleted {
                                HistoryRetentionAnchor::Deleted(at)
                            } else {
                                HistoryRetentionAnchor::Closed(at)
                            }
                        });
                    if !consider(evidence.as_ref(), anchor)? {
                        break;
                    }
                }
            }
            _ => {
                for row in &self.history {
                    let row_family = match row.value {
                        MemoryHistoryValue::Collection(_) => StorageIdentityHistoryKind::Collection,
                        MemoryHistoryValue::Class(_) => StorageIdentityHistoryKind::Class,
                        MemoryHistoryValue::Object(_) => StorageIdentityHistoryKind::Object,
                        MemoryHistoryValue::ExportTemplate(_) => {
                            StorageIdentityHistoryKind::ExportTemplate
                        }
                        MemoryHistoryValue::RemoteTarget(_) => {
                            StorageIdentityHistoryKind::RemoteTarget
                        }
                    };
                    if row_family != family {
                        continue;
                    }
                    let anchor = if row.operation == StorageHistoryOperation::Delete {
                        Some(HistoryRetentionAnchor::Deleted(row.valid_from))
                    } else {
                        row.valid_to.map(HistoryRetentionAnchor::Closed)
                    };
                    if !consider(Some(&row.logical_identity), anchor)? {
                        break;
                    }
                }
            }
        }
        HistoryLifecycleScanResult::new(
            candidates,
            scanned,
            !exhausted && scanned < scan.limit(),
            unresolved,
        )
        .map_err(invalid_contract_value)
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    #[tokio::test]
    async fn policy_control_and_canonical_audit_commit_together() {
        let storage = MemoryStorage::new();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let hold = HistoryRetentionHold::new(
            Uuid::new_v4(),
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Object,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            Utc::now(),
            None,
            PrincipalIdentity::generate(),
        )
        .unwrap();
        let change = snapshot
            .control()
            .create_hold(hold, snapshot.fence())
            .unwrap();
        let expected_id = change.next().audit_events_since(change.expected()).unwrap()[0]
            .event_id()
            .as_uuid();
        storage.commit_history_lifecycle(change).await.unwrap();
        let state = storage.state.read_control().await;
        assert!(
            state
                .events
                .iter()
                .any(|event| event.as_parts().0.event_id() == expected_id)
        );
    }

    #[tokio::test]
    async fn audit_sequence_failure_rolls_back_new_hold_authority() {
        let storage = MemoryStorage::new();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let hold = HistoryRetentionHold::new(
            Uuid::new_v4(),
            HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Object,
                HistoryPolicyScope::Deployment,
            )
            .unwrap(),
            Utc::now(),
            None,
            PrincipalIdentity::generate(),
        )
        .unwrap();
        let change = snapshot
            .control()
            .create_hold(hold, snapshot.fence())
            .unwrap();
        storage.state.write_control().await.next_event_sequence = i64::MAX;
        assert!(storage.commit_history_lifecycle(change).await.is_err());
        assert_eq!(
            storage.capture_history_lifecycle().await.unwrap().control(),
            snapshot.control()
        );
    }
}

impl MemoryState {
    fn history_policy_scope_known(&self, scope: HistoryPolicyScope) -> Result<bool, StorageError> {
        if self.history_lifecycle.retains_scope(scope) {
            return Ok(true);
        }
        let (family, identity) = match scope {
            HistoryPolicyScope::Deployment => return Ok(true),
            HistoryPolicyScope::Class { identity } => {
                (StorageIdentityResourceKind::Class, identity)
            }
            HistoryPolicyScope::Collection { identity } => {
                (StorageIdentityResourceKind::Collection, identity)
            }
        };
        let ids: Box<dyn Iterator<Item = i32> + '_> = match family {
            StorageIdentityResourceKind::Class => Box::new(self.classes.keys().copied()),
            _ => Box::new(self.collections.keys().copied()),
        };
        for id in ids {
            let evidence = self
                .logical_identities
                .record(StorageIdentityTarget::Resource {
                    kind: family,
                    id: ResourceId::new(id)
                        .map_err(|error| StorageError::invalid_input(error.to_string()))?,
                })?;
            if evidence
                .subject()
                .and_then(|subject| subject.record_identity())
                .is_some_and(|record| record.resource() == identity)
            {
                return Ok(true);
            }
        }
        Ok(self.history.iter().any(|entry| {
            let matches_family = matches!(
                (&entry.value, family),
                (
                    MemoryHistoryValue::Class(_),
                    StorageIdentityResourceKind::Class
                ) | (
                    MemoryHistoryValue::Collection(_),
                    StorageIdentityResourceKind::Collection
                )
            );
            matches_family
                && entry
                    .logical_identity
                    .subject()
                    .and_then(|subject| subject.record_identity())
                    .is_some_and(|record| record.resource() == identity)
        }))
    }
}

#[cfg(test)]
mod online_read_tests {
    use super::*;

    async fn prepared_fixture() -> (
        MemoryStorage,
        MemoryHistoryEntry,
        RuntimeRegistration,
        HistoryLifecycleControlChange,
    ) {
        let storage = MemoryStorage::new();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Object,
            HistoryPolicyScope::Deployment,
        )
        .unwrap();
        let principal = PrincipalIdentity::generate();
        let policy = HistoryPolicyChange::new(
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
        let change = snapshot
            .control()
            .apply(
                HistoryPolicyApplyRequest::new(policy, None),
                principal,
                snapshot.fence(),
                snapshot.observed_at(),
            )
            .unwrap();
        storage.commit_history_lifecycle(change).await.unwrap();
        let registration = storage.register_history_runtime(None).await.unwrap();
        let (storage, _, entry) = crate::deletion_control::tests::fixture_on(storage, true).await;
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let until = entry.valid_to.unwrap();
        let candidate = HistoryExpiryCandidate::from_evidence(
            StorageIdentityHistoryKind::Object,
            &entry.logical_identity,
            Some(HistoryRetentionAnchor::Closed(until)),
            false,
        )
        .unwrap()
        .unwrap()
        .with_read_span(HistoryExpiryReadSpan::closed(entry.valid_from, until).unwrap())
        .unwrap();
        let bytes = serde_json::to_vec(&entry.backup_history_row().unwrap().1)
            .unwrap()
            .len();
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual { principal },
            HistoryLifecycleScan::new(key, 1).unwrap(),
            &snapshot,
        )
        .unwrap();
        assert!(matches!(
            selection
                .consider(
                    candidate,
                    HistoryExpiryInputBytes::new(bytes as u64).unwrap(),
                    &[]
                )
                .unwrap(),
            HistoryExpirySelectionOutcome::Selected
        ));
        let change = snapshot
            .control()
            .admit_expiry(selection.finish().unwrap(), snapshot.fence())
            .unwrap();
        (storage, entry, registration, change)
    }
    async fn fixture() -> (MemoryStorage, MemoryHistoryEntry, RuntimeRegistration) {
        let (storage, entry, registration, change) = prepared_fixture().await;
        storage.commit_history_lifecycle(change).await.unwrap();
        (storage, entry, registration)
    }
    #[tokio::test]
    async fn expired_copy_is_omitted_from_object_history_list() {
        let (storage, entry, registration) = fixture().await;
        let MemoryHistoryValue::Object(record) = &entry.value else {
            unreachable!()
        };
        let page = storage
            .list_object_history(StorageObjectHistoryListQuery::new(
                record.id(),
                record.class_id(),
                QueryOptions::empty(),
                StorageHistoryCollectionScope::All,
            ))
            .await
            .unwrap();
        assert_eq!(page.rows().len(), 1);
        assert!(
            page.rows()
                .iter()
                .all(|row| row.clone().into_parts().1.valid_from() != entry.valid_from)
        );
        storage.unregister_runtime(registration).await.unwrap();
    }
    #[tokio::test]
    async fn restored_source_alias_is_not_republished_by_history_list() {
        let (storage, entry, registration) = fixture().await;
        let mut bytes = *hubuum_domain::VersionIdentity::generate().as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x80;
        let derived = hubuum_domain::VersionIdentity::from_bytes(bytes).unwrap();
        {
            // Manufacture only the restored history copy. Full restore exposure
            // and live replacement gates require separate native qualification.
            let mut state = storage.state.write_control().await;
            let restored = state
                .history
                .iter_mut()
                .find(|copy| copy.logical_identity == entry.logical_identity)
                .unwrap();
            restored.logical_identity = restored
                .logical_identity
                .clone()
                .at_restore_baseline(derived)
                .unwrap();
        }
        let MemoryHistoryValue::Object(record) = &entry.value else {
            unreachable!()
        };
        let page = storage
            .list_object_history(StorageObjectHistoryListQuery::new(
                record.id(),
                record.class_id(),
                QueryOptions::empty(),
                StorageHistoryCollectionScope::All,
            ))
            .await
            .unwrap();
        assert!(
            page.rows()
                .iter()
                .all(|row| row.clone().into_parts().1.valid_from() != entry.valid_from)
        );
        storage.unregister_runtime(registration).await.unwrap();
    }

    #[tokio::test]
    async fn expired_as_of_copy_reports_a_gap_instead_of_resurrecting_a_value() {
        let (storage, entry, registration) = fixture().await;
        let MemoryHistoryValue::Object(record) = &entry.value else {
            unreachable!()
        };
        let Err(error) = storage
            .get_object_history_as_of(StorageObjectHistoryAsOfQuery::new(
                record.id(),
                record.class_id(),
                entry.valid_from,
            ))
            .await
        else {
            panic!("expired as-of copy must report an unavailable gap");
        };
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
        storage.unregister_runtime(registration).await.unwrap();
    }
    #[tokio::test]
    async fn registered_runtime_keeps_the_newer_as_of_version_readable() {
        let (storage, entry, registration) = fixture().await;
        let MemoryHistoryValue::Object(record) = &entry.value else {
            unreachable!()
        };
        let current = storage
            .get_object_history_as_of(StorageObjectHistoryAsOfQuery::new(
                record.id(),
                record.class_id(),
                Utc::now(),
            ))
            .await
            .unwrap();
        assert!(current.is_some());
        storage.unregister_runtime(registration).await.unwrap();
    }
    #[tokio::test]
    async fn backup_capture_omits_the_exact_expired_history_copy() {
        let (storage, entry, registration) = fixture().await;
        let replay = storage
            .capture_backup_replay(
                true,
                StorageBackupBudget::new(16 * 1024 * 1024, 10000).unwrap(),
            )
            .await
            .unwrap();
        let target = entry
            .logical_identity
            .subject()
            .unwrap()
            .record_identity()
            .unwrap();
        let rows = replay.history_rows(StorageBackupHistorySection::ObjectHistory);
        for row in rows {
            let row = row.unwrap();
            let evidence: StorageIdentityEvidence =
                serde_json::from_value(row.get("logical_identity").unwrap().clone()).unwrap();
            assert_ne!(evidence.subject().unwrap().record_identity(), Some(target));
        }
        storage.unregister_runtime(registration).await.unwrap();
    }
    #[tokio::test]
    async fn mixed_protocol_fleet_blocks_admission_until_legacy_owner_unregisters() {
        let (storage, _, aware, change) = prepared_fixture().await;
        let legacy = storage.register_runtime().await.unwrap();
        let error = storage
            .commit_history_lifecycle(change.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        storage.unregister_runtime(legacy).await.unwrap();
        storage.commit_history_lifecycle(change).await.unwrap();
        storage.unregister_runtime(aware).await.unwrap();
    }
    #[tokio::test]
    async fn retained_expiry_refuses_legacy_startup() {
        let (storage, _, aware) = fixture().await;
        let Err(error) = storage.register_runtime().await else {
            panic!("retained expiry must refuse legacy startup");
        };
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        storage.unregister_runtime(aware).await.unwrap();
    }
    #[tokio::test]
    async fn history_registration_preserves_isolation_and_protocol() {
        let storage = MemoryStorage::new();
        let isolation = RuntimeIsolationDomain::new(Uuid::new_v4(), 1, 1).unwrap();
        let aware = storage
            .register_history_runtime(Some(isolation.clone()))
            .await
            .unwrap();
        {
            let state = storage.state.read_control().await;
            let retained = &state.runtime_registrations[&aware.id()];
            assert_eq!(retained.isolation.as_ref(), Some(&isolation));
            assert_eq!(retained.history_protocol, HistoryRuntimeProtocol::OnlineV1);
        }
        storage.unregister_runtime(aware).await.unwrap();
    }
}
