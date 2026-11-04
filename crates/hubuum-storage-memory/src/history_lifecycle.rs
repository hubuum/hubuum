use super::*;
use serde_json::Value;

#[async_trait]
impl HistoryLifecycleStorage for MemoryStorage {
    async fn admit_history_publication(
        &self,
        kind: HistoryPublicationKind,
    ) -> Result<HistoryPublicationOwner, StorageError> {
        let scope = self.current_execution_scope();
        if scope.history_read_override().is_some() {
            return Err(StorageError::conflict(
                "Nested work must inherit its original history publication owner",
            ));
        }
        let application = scope
            .application_access_override()
            .cloned()
            .ok_or_else(|| {
                StorageError::unavailable("Publication requires captured application admission")
            })?;
        let mut state = self.state.write_history_admission().await?;
        state.ensure_application_permit(&application)?;
        if !application
            .owner()
            .and_then(|owner| state.runtime_registrations.get(&owner.id()))
            .is_some_and(|runtime| runtime.history_protocol.supports_online())
        {
            return Err(StorageError::unavailable(
                "Publication requires a history-aware registered runtime",
            ));
        }
        let epoch = HistoryReadEpoch::new(
            application,
            state.history_lifecycle_snapshot()?.fence(),
            state.history_publications.generation(),
        )
        .map_err(invalid_contract_value)?;
        state.history_publications.admit(kind, epoch)
    }
    async fn settle_history_publication(
        &self,
        settlement: HistoryPublicationSettlement,
    ) -> Result<(), StorageError> {
        self.state
            .write_control()
            .await
            .history_publications
            .settle(settlement)
    }
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
        let fallback = StorageExecutionPermit::new(
            None,
            ApplicationFence::new(
                change.fence().incarnation(),
                change.fence().deletion_revision().value() as i64,
            )
            .map_err(invalid_contract_value)?,
        );
        let mut state = self.state.write_history_control(&fallback).await?;
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
        let registration = match history_protocol {
            HistoryRuntimeProtocol::Legacy => RuntimeRegistration::generate(),
            HistoryRuntimeProtocol::OnlineV1 => RuntimeRegistration::generate_history_aware(),
        };
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
    /// Never equate history-query filtering with restored-image exposure proof.
    /// Call under the state write lock; no current/open copy is removed here.
    fn check_history_exposure_copy(
        journal: &HistoryExpiryJournal,
        budget: &mut HistoryRecoveryExposureBudget,
        family: StorageIdentityHistoryKind,
        evidence: &StorageIdentityEvidence,
    ) -> Result<(), StorageError> {
        budget
            .observe_evidence(evidence)
            .map_err(|error| StorageError::unavailable(error.to_string()))?;
        if journal.excludes_evidence(family, Some(evidence)) {
            return Err(StorageError::unavailable(
                "Current or protected-open copy carries expired or unresolved history ancestry; keep quarantine for explicit reconciliation",
            ));
        }
        Ok(())
    }

    pub(super) fn require_history_exposure_safe(&self) -> Result<(), StorageError> {
        if self.history_publications.active() != 0 {
            return Err(StorageError::unavailable(
                "History publication drivers or producers lack positive retirement; keep quarantine",
            ));
        }
        let journal = self.history_lifecycle.authority().expiry();
        if journal.intents().is_empty() {
            return Ok(());
        }
        let families = journal
            .intents()
            .iter()
            .flat_map(|intent| intent.manifest().members())
            .map(HistoryExpiryMember::family)
            .collect::<BTreeSet<_>>();
        let mut budget = HistoryRecoveryExposureBudget::default();
        use StorageIdentityHistoryKind as Family;
        for family in &families {
            let (kind, ids): (
                StorageIdentityResourceKind,
                Box<dyn Iterator<Item = i32> + '_>,
            ) = match family {
                Family::Collection => (
                    StorageIdentityResourceKind::Collection,
                    Box::new(self.collections.keys().copied()),
                ),
                Family::Class => (
                    StorageIdentityResourceKind::Class,
                    Box::new(self.classes.keys().copied()),
                ),
                Family::Object => (
                    StorageIdentityResourceKind::Object,
                    Box::new(self.objects.keys().copied()),
                ),
                Family::ClassRelation => (
                    StorageIdentityResourceKind::ClassRelation,
                    Box::new(self.class_relations.keys().copied()),
                ),
                Family::ObjectRelation => (
                    StorageIdentityResourceKind::ObjectRelation,
                    Box::new(self.object_relations.keys().copied()),
                ),
                Family::ExportTemplate => (
                    StorageIdentityResourceKind::ExportTemplate,
                    Box::new(self.export_templates.keys().copied()),
                ),
                Family::RemoteTarget => (
                    StorageIdentityResourceKind::RemoteTarget,
                    Box::new(self.remote_targets.keys().copied()),
                ),
                Family::ComputedDefinitions | Family::ClassSchema => continue,
            };
            for id in ids {
                let evidence =
                    self.logical_identities
                        .record_ref(StorageIdentityTarget::Resource {
                            kind,
                            id: ResourceId::new(id).map_err(|error| {
                                StorageError::backend_failure(error.to_string())
                            })?,
                        })?;
                Self::check_history_exposure_copy(journal, &mut budget, *family, evidence)?;
            }
        }
        for entry in &self.history {
            if families.contains(&entry.family())
                && entry.valid_to.is_none()
                && entry.operation != StorageHistoryOperation::Delete
            {
                Self::check_history_exposure_copy(
                    journal,
                    &mut budget,
                    entry.family(),
                    &entry.logical_identity,
                )?;
            }
        }
        for (section, family) in [
            (
                StorageBackupHistorySection::ClassRelationHistory,
                Family::ClassRelation,
            ),
            (
                StorageBackupHistorySection::ObjectRelationHistory,
                Family::ObjectRelation,
            ),
        ] {
            if !families.contains(&family) {
                continue;
            }
            for row in self.relation_history.get(&section).into_iter().flatten() {
                let until = row.get("valid_to").ok_or_else(|| {
                    StorageError::unavailable("Missing open-copy interval; keep quarantine")
                })?;
                let operation = row
                    .get("operation")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        StorageError::unavailable("Missing open-copy operation; keep quarantine")
                    })?;
                if !matches!(operation, "create" | "update" | "delete") {
                    return Err(StorageError::unavailable(
                        "Unknown open-copy operation; keep quarantine",
                    ));
                }
                if !until.is_null() || operation == "delete" {
                    continue;
                }
                let raw = row.get("logical_identity").ok_or_else(|| {
                    StorageError::unavailable("Missing open-copy correspondence; keep quarantine")
                })?;
                budget
                    .observe_json(raw)
                    .map_err(|error| StorageError::unavailable(error.to_string()))?;
                let evidence: StorageIdentityEvidence = serde_json::from_value(raw.clone())
                    .map_err(|_| {
                        StorageError::unavailable(
                            "Invalid open-copy correspondence; keep quarantine",
                        )
                    })?;
                if journal.excludes_evidence(family, Some(&evidence)) {
                    return Err(StorageError::unavailable(
                        "Protected-open copy carries expired or unresolved history ancestry; keep quarantine for explicit reconciliation",
                    ));
                }
            }
        }
        if families.contains(&Family::ComputedDefinitions) {
            for entry in &self.computed_history {
                if entry.valid_to().is_none() {
                    let evidence = entry.logical_identity().ok_or_else(|| {
                        StorageError::unavailable(
                            "Missing current computed correspondence; keep quarantine",
                        )
                    })?;
                    Self::check_history_exposure_copy(
                        journal,
                        &mut budget,
                        Family::ComputedDefinitions,
                        evidence,
                    )?;
                }
            }
        }
        if families.contains(&Family::ClassSchema) {
            for &(class_id, revision) in self.schema_revisions.keys() {
                let evidence =
                    self.logical_identities
                        .record_ref(StorageIdentityTarget::SchemaRevision {
                            class_id: ClassId::new(class_id).map_err(|error| {
                                StorageError::backend_failure(error.to_string())
                            })?,
                            revision: SchemaRevision::new(revision).map_err(|error| {
                                StorageError::backend_failure(error.to_string())
                            })?,
                        })?;
                Self::check_history_exposure_copy(
                    journal,
                    &mut budget,
                    Family::ClassSchema,
                    evidence,
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn require_proven_empty_history(
        &self,
        family: StorageIdentityHistoryKind,
        entity_id: ResourceId,
        expected: Option<ResourceIdentity>,
        at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let Some(expected) = expected else {
            return Ok(());
        };
        let (kind, created) = match family {
            StorageIdentityHistoryKind::Collection => (
                StorageIdentityResourceKind::Collection,
                self.collections
                    .get(&entity_id.id())
                    .map(|value| value.created_at()),
            ),
            StorageIdentityHistoryKind::Object => (
                StorageIdentityResourceKind::Object,
                self.objects
                    .get(&entity_id.id())
                    .map(|value| value.created_at()),
            ),
            StorageIdentityHistoryKind::Class => (
                StorageIdentityResourceKind::Class,
                self.classes
                    .get(&entity_id.id())
                    .map(|value| value.created_at()),
            ),
            StorageIdentityHistoryKind::ExportTemplate => (
                StorageIdentityResourceKind::ExportTemplate,
                self.export_templates
                    .get(&entity_id.id())
                    .map(|value| value.metadata().created_at()),
            ),
            StorageIdentityHistoryKind::RemoteTarget => (
                StorageIdentityResourceKind::RemoteTarget,
                self.remote_targets
                    .get(&entity_id.id())
                    .map(|value| value.metadata().created_at()),
            ),
            _ => {
                return Err(StorageError::unavailable(
                    "Unsupported canonical empty-history proof",
                ));
            }
        };
        if !created.is_some_and(|created| at < created) {
            return Err(StorageError::unavailable(
                "Historical coverage for the original resource is unavailable",
            ));
        }
        let canonical = self
            .logical_identities
            .record(StorageIdentityTarget::Resource {
                kind,
                id: entity_id,
            })?;
        if canonical
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .is_none_or(|identity| identity.resource() != expected)
        {
            return Err(StorageError::unavailable(
                "Original canonical resource correspondence changed",
            ));
        }
        Ok(())
    }

    pub(super) fn require_population_class_binding(
        &self,
        query: &StorageClassObjectsAsOfQuery,
    ) -> Result<(), StorageError> {
        let Some(expected) = query.expected_resource() else {
            return self.require_history_resource_binding(
                StorageIdentityHistoryKind::Class,
                None,
                query.at(),
                None,
            );
        };
        if !self.classes.contains_key(&query.class_id().id()) {
            return Err(StorageError::unavailable(
                "Original population class correspondence changed",
            ));
        }
        let canonical = self
            .logical_identities
            .record(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Class,
                id: ResourceId::new(query.class_id().id())
                    .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            })?;
        if canonical
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .is_none_or(|identity| identity.resource() != expected)
        {
            return Err(StorageError::unavailable(
                "Original population class correspondence changed",
            ));
        }
        Ok(())
    }

    pub(super) fn require_population_gap_authorization(
        &self,
        query: &StorageClassObjectsAsOfQuery,
    ) -> Result<(), StorageError> {
        let context = MEMORY_EXECUTION_SCOPE
            .try_with(|scope| scope.history_read_override().cloned())
            .unwrap_or(None);
        let Some(context) = context else {
            return Ok(());
        };
        if self.history_lifecycle_snapshot()?.fence() != context.epoch().history() {
            return Err(StorageError::unavailable(
                "Original population history authority changed",
            ));
        }
        let gaps = HistoryObjectPopulationGap::for_population(
            self.history_lifecycle.authority().expiry(),
            query,
            context.epoch().clone(),
        )
        .map_err(|_| {
            StorageError::unavailable(
                "Retained population lacks bounded original authorization facts",
            )
        })?;
        let mut bytes = 0;
        for gap in gaps {
            if let Some(current) = self.objects.get(&gap.facts().object_id().id()) {
                if matches!(query.collection_scope(), StorageHistoryCollectionScope::Visible(ids)
                    if !ids.contains(&current.collection_id()))
                {
                    continue;
                }
                let identity = self
                    .logical_identities
                    .record(StorageIdentityTarget::Resource {
                        kind: StorageIdentityResourceKind::Object,
                        id: ResourceId::new(current.id().id())
                            .map_err(|error| StorageError::backend_failure(error.to_string()))?,
                    })?;
                let encoded = serde_json::to_vec(&identity)
                    .map_err(|_| {
                        StorageError::backend_failure("Cannot measure population identity")
                    })?
                    .len();
                bytes += encoded;
                if encoded > 65536 || bytes > HistoryObjectPopulationGap::MAX_FACT_BYTES {
                    return Err(StorageError::unavailable(
                        "Original population object correspondence exceeds its bound",
                    ));
                }
                if identity
                    .subject()
                    .and_then(StorageIdentitySubject::record_identity)
                    .is_none_or(|target| target.resource() != gap.facts().target().resource())
                {
                    return Err(StorageError::unavailable(
                        "Original population object correspondence changed",
                    ));
                }
            } else if !query.include_deleted() {
                continue;
            }
            return Err(StorageError::history_population_gap(gap));
        }
        Ok(())
    }

    pub(super) fn require_history_resource_binding(
        &self,
        family: StorageIdentityHistoryKind,
        expected: Option<ResourceIdentity>,
        at: DateTime<Utc>,
        selected: Option<&MemoryHistoryEntry>,
    ) -> Result<(), StorageError> {
        if expected.is_none()
            && MEMORY_EXECUTION_SCOPE
                .try_with(|scope| scope.history_read_override().is_some())
                .unwrap_or(false)
        {
            return Err(StorageError::unavailable(
                "Online history reads require the original logical resource identity",
            ));
        }
        let Some(expected) = expected else {
            return Ok(());
        };
        if let Some(selected) = selected {
            let actual = selected
                .logical_identity
                .subject()
                .and_then(StorageIdentitySubject::record_identity)
                .ok_or_else(|| {
                    StorageError::unavailable("Historical resource correspondence is unresolved")
                })?;
            if selected.family() != family || actual.resource() != expected {
                return Err(StorageError::not_found(
                    "Historical version differs from the original resource",
                ));
            }
        } else if self
            .history_lifecycle
            .authority()
            .expiry()
            .restriction_as_of(family, expected, at, None)
            .is_some()
        {
            return Err(StorageError::not_found("Historical version is expired"));
        }
        Ok(())
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
    pub(super) fn history_lifecycle_snapshot(
        &self,
    ) -> Result<HistoryLifecycleSnapshot, StorageError> {
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

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn publication_registry_requires_original_owner_for_productive_entry(
        #[case] bind_handle: bool,
    ) {
        let storage = MemoryStorage::new();
        let registration = storage.register_history_runtime(None).await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let storage = if bind_handle {
            storage.with_runtime_owner(registration.owner())
        } else {
            storage
        };
        let scope = StorageExecutionScope::default().with_application_access(permit);
        let result = storage
            .run_in_scope(scope, storage.get_collection(CollectionId::new(1).unwrap()))
            .await;
        storage.unregister_runtime(registration).await.unwrap();
        assert!(
            result.is_err(),
            "Aware scopes require their publication owner"
        );
    }

    #[tokio::test]
    async fn publication_registry_unretired_owner_blocks_exposure_without_expiry() {
        let storage = MemoryStorage::new();
        let registration = storage.register_history_runtime(None).await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let owner = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(permit),
                storage.admit_history_publication(HistoryPublicationKind::Backup),
            )
            .await
            .unwrap();
        let blocked = storage
            .state
            .read_control()
            .await
            .require_history_exposure_safe();
        // Logical registry fixture only: no external producer exists.
        let settlement = owner
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage
            .settle_history_publication(settlement)
            .await
            .unwrap();
        let cleared = storage
            .state
            .read_control()
            .await
            .require_history_exposure_safe();
        storage.unregister_runtime(registration).await.unwrap();
        assert!(blocked.is_err());
        assert!(cleared.is_ok());
    }

    #[tokio::test]
    async fn publication_registry_round_trip_rejects_retired_scope() {
        let storage = MemoryStorage::new();
        let registration = storage.register_history_runtime(None).await.unwrap();
        let storage = storage.with_runtime_owner(registration.owner());
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(permit);
        let owner = storage
            .run_in_scope(
                scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::TaskOutput),
            )
            .await
            .unwrap();
        let productive = scope.with_history_read(owner.context());
        let root = CollectionId::new(1).unwrap();
        storage
            .run_in_scope(productive.clone(), storage.get_collection(root))
            .await
            .unwrap();
        let settlement = owner
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage
            .settle_history_publication(settlement)
            .await
            .unwrap();
        let retired = storage
            .run_in_scope(productive, storage.get_collection(root))
            .await;
        storage.unregister_runtime(registration).await.unwrap();
        assert!(retired.is_err());
    }
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
        let mut consider = |evidence: Option<&StorageIdentityEvidence>,
                            anchor,
                            from,
                            until|
         -> Result<bool, StorageError> {
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
                    Some(candidate) if scan.includes(&candidate) => candidates.push(
                        candidate
                            .with_native_interval(from, until)
                            .map_err(invalid_contract_value)?,
                    ),
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
                    let from = anchor.map(HistoryRetentionAnchor::at).ok_or_else(|| {
                        StorageError::backend_failure("Missing schema history occurrence clock")
                    })?;
                    if !consider(evidence.as_ref(), anchor, from, None)? {
                        break;
                    }
                }
            }
            StorageIdentityHistoryKind::ComputedDefinitions => {
                for row in &self.computed_history {
                    if !consider(
                        row.logical_identity(),
                        row.valid_to().map(HistoryRetentionAnchor::Closed),
                        row.valid_from(),
                        row.valid_to(),
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
                    let from = row
                        .get("valid_from")
                        .cloned()
                        .map(serde_json::from_value::<DateTime<Utc>>)
                        .transpose()
                        .map_err(|_| {
                            StorageError::backend_failure("Invalid relation history start")
                        })?
                        .ok_or_else(|| {
                            StorageError::backend_failure("Missing relation history start")
                        })?;
                    let until = row
                        .get("valid_to")
                        .filter(|value| !value.is_null())
                        .cloned()
                        .map(serde_json::from_value::<DateTime<Utc>>)
                        .transpose()
                        .map_err(|_| {
                            StorageError::backend_failure("Invalid relation history end")
                        })?;
                    if !consider(evidence.as_ref(), anchor, from, until)? {
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
                    if !consider(
                        Some(&row.logical_identity),
                        anchor,
                        row.valid_from,
                        row.valid_to,
                    )? {
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
    #[rstest::rstest]
    #[case::current_collision("current", true)]
    #[case::protected_open_collision("open", true)]
    #[case::clean_descendant("clean", false)]
    #[case::unknown_ancestry("unknown", true)]
    #[tokio::test]
    async fn exposure_gate_retains_quarantine_without_modifying_copies(
        #[case] mode: &str,
        #[case] blocked: bool,
    ) {
        let (storage, closed, registration) = fixture().await;
        let MemoryHistoryValue::Object(record) = &closed.value else {
            unreachable!()
        };
        let derived = || {
            let mut bytes = *VersionIdentity::generate().as_bytes();
            bytes[6] = (bytes[6] & 0x0f) | 0x80;
            VersionIdentity::from_bytes(bytes).unwrap()
        };
        {
            let mut state = storage.state.write_control().await;
            let live = state.objects[&record.id().id()].clone();
            let original = state
                .logical_identities
                .record(StorageIdentityTarget::Resource {
                    kind: StorageIdentityResourceKind::Object,
                    id: ResourceId::new(record.id().id()).unwrap(),
                })
                .unwrap();
            let clean = original.at_restore_baseline(derived()).unwrap();
            let collision = closed
                .logical_identity
                .clone()
                .at_restore_baseline(derived())
                .unwrap();
            let current = match mode {
                "current" => collision.clone(),
                "unknown" => {
                    StorageIdentityEvidence::try_new(clean.subject(), Default::default()).unwrap()
                }
                _ => clean.clone(),
            };
            // Model restored correspondence only, not a full restore or protected
            // admission. Native replay/exposure evidence remains independently required.
            state
                .logical_identities
                .restore_record(&live, current.clone());
            let open = state
                .history
                .iter_mut()
                .find(|entry| {
                    entry.value.entity_id() == record.id().id()
                        && matches!(entry.value, MemoryHistoryValue::Object(_))
                        && entry.valid_to.is_none()
                })
                .unwrap();
            open.logical_identity = if mode == "open" { collision } else { clean };
            state.application_control =
                ApplicationControlState::quarantined(state.application_control.fence());
            let retained = state.history_lifecycle.clone();
            let copies = state
                .history
                .iter()
                .map(|entry| {
                    (
                        entry.logical_identity.clone(),
                        entry.valid_from,
                        entry.valid_to,
                    )
                })
                .collect::<Vec<_>>();
            let result = state.require_history_exposure_safe();
            assert_eq!(result.is_err(), blocked);
            assert!(
                !state
                    .application_control
                    .is_released_for(&state.deletion_control)
            );
            assert_eq!(state.history_lifecycle, retained);
            assert_eq!(
                state
                    .history
                    .iter()
                    .map(|entry| (
                        entry.logical_identity.clone(),
                        entry.valid_from,
                        entry.valid_to
                    ))
                    .collect::<Vec<_>>(),
                copies
            );
            assert_eq!(
                state
                    .logical_identities
                    .record(StorageIdentityTarget::Resource {
                        kind: StorageIdentityResourceKind::Object,
                        id: ResourceId::new(record.id().id()).unwrap(),
                    })
                    .unwrap(),
                current
            );
        }
        storage.unregister_runtime(registration).await.unwrap();
    }

    #[rstest::rstest]
    #[case::before_creation(false, true, true)]
    #[case::changed_uuid(true, true, false)]
    #[case::unproved_gap(false, false, false)]
    #[tokio::test]
    async fn original_canonical_creation_is_required_for_empty_history(
        #[case] changed: bool,
        #[case] before: bool,
        #[case] empty: bool,
    ) {
        let storage = MemoryStorage::new();
        let id = ResourceId::new(1).unwrap();
        let original = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id,
            })
            .await
            .unwrap()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
            .resource();
        let created = storage
            .get_collection(CollectionId::new(1).unwrap())
            .await
            .unwrap()
            .created_at();
        // Deliberately model missing coverage, not a protected removal. This
        // does not qualify native physical expiry or a publisher drain.
        storage.state.write_control().await.history.clear();
        let requested = if changed {
            ResourceIdentity::generate()
        } else {
            original
        };
        let at = created
            + if before {
                -Duration::microseconds(1)
            } else {
                Duration::microseconds(1)
            };
        let result = storage
            .get_collection_history_as_of(
                StorageHistoryAsOfQuery::new(id, at).for_resource(requested),
            )
            .await;
        if empty {
            assert!(matches!(result, Ok(None)));
        } else {
            let Err(error) = result else {
                panic!("Unproved coverage cannot produce a complete empty result");
            };
            assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        }
    }

    #[rstest::rstest]
    #[case::before(-1, false, false)]
    #[case::inclusive_start(0, false, true)]
    #[case::exclusive_end(1, false, false)]
    #[case::other_resource(0, true, false)]
    #[tokio::test]
    async fn missing_copy_uses_original_uuid_and_interval(
        #[case] boundary: i32,
        #[case] unrelated: bool,
        #[case] expired: bool,
    ) {
        let (storage, entry, registration) = fixture().await;
        let original = entry
            .logical_identity
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
            .resource();
        let resource = if unrelated {
            ResourceIdentity::generate()
        } else {
            original
        };
        let at = match boundary {
            -1 => entry.valid_from - Duration::microseconds(1),
            0 => entry.valid_from,
            _ => entry.valid_to.unwrap(),
        };
        let result = storage
            .state
            .read_control()
            .await
            .require_history_resource_binding(
                StorageIdentityHistoryKind::Object,
                Some(resource),
                at,
                None,
            );
        storage.unregister_runtime(registration).await.unwrap();
        assert_eq!(result.is_err(), expired);
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
    #[rstest::rstest]
    #[case::expired_removed(true, StorageErrorKind::NotFound)]
    #[case::unproved_gap(false, StorageErrorKind::Unavailable)]
    #[tokio::test]
    async fn original_object_binding_rejects_missing_payload_as_ordinary_absence(
        #[case] retained: bool,
        #[case] expected_error: StorageErrorKind,
    ) {
        let (storage, entry, registration, change) = prepared_fixture().await;
        if retained {
            storage.commit_history_lifecycle(change).await.unwrap();
        }
        let original = entry
            .logical_identity
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
            .resource();
        let MemoryHistoryValue::Object(record) = &entry.value else {
            unreachable!()
        };
        // Missing-payload fixture only, not physical expiry/drain qualification.
        storage
            .state
            .write_control()
            .await
            .history
            .retain(|copy| copy.logical_identity != entry.logical_identity);
        let result = storage
            .get_object_history_as_of(
                StorageObjectHistoryAsOfQuery::new(
                    record.id(),
                    record.class_id(),
                    entry.valid_from,
                )
                .for_resource(original),
            )
            .await;
        storage.unregister_runtime(registration).await.unwrap();
        assert!(matches!(result, Err(error) if error.kind() == expected_error));
    }

    #[rstest::rstest]
    #[case::original(false, false)]
    #[case::stale(true, false)]
    #[case::missing(false, true)]
    #[tokio::test]
    async fn population_rechecks_original_class_before_selecting_rows(
        #[case] stale: bool,
        #[case] missing: bool,
    ) {
        let (storage, entry, registration, _) = prepared_fixture().await;
        let MemoryHistoryValue::Object(record) = &entry.value else {
            unreachable!()
        };
        let original = storage
            .state
            .read()
            .await
            .unwrap()
            .logical_identities
            .record(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Class,
                id: ResourceId::new(record.class_id().id()).unwrap(),
            })
            .unwrap()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
            .resource();
        let query = StorageClassObjectsAsOfQuery::try_new(
            if missing {
                ClassId::new(i32::MAX).unwrap()
            } else {
                record.class_id()
            },
            entry.valid_from,
            QueryOptions::new(vec![], vec![], Some(10), None, false).unwrap(),
        )
        .unwrap()
        .for_resource(if stale {
            ResourceIdentity::generate()
        } else {
            original
        })
        .visibility(StorageHistoryCollectionScope::All, true);
        let result = storage.list_class_objects_as_of(query).await;
        storage.unregister_runtime(registration).await.unwrap();
        if stale || missing {
            assert_eq!(result.err().unwrap().kind(), StorageErrorKind::Unavailable);
        } else {
            assert!(!result.unwrap().into_parts().0.is_empty());
        }
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
