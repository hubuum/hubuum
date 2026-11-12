mod expiry;
use super::*;
use serde_json::Value;

#[async_trait]
impl HistoryLifecycleStorage for MemoryStorage {
    async fn reapply_history_expiry(
        &self,
        request: HistoryExpiryReapplication,
    ) -> Result<bool, StorageError> {
        let scope = self.current_execution_scope();
        if scope.storage_callback_active() || scope.history_read_override().is_some() {
            return Err(StorageError::unavailable(
                "History reapplication requires an independent control scope",
            ));
        }
        let mut guard = self.state.write_control().await;
        if guard
            .application_control
            .is_released_for(&guard.deletion_control)
        {
            return Err(StorageError::unavailable(
                "History expiry reapplication requires quarantine",
            ));
        }
        guard.require_runtime_drain()?;
        guard.require_history_exposure_safe()?;
        let mut state = guard.clone();
        let snapshot = state.history_lifecycle_snapshot()?;
        request
            .validate(&snapshot)
            .map_err(invalid_contract_value)?;
        let removed = expiry::remove_member_on(
            &mut state,
            request.member(),
            request.batch(),
            &snapshot,
            request.protection(),
        )?;
        *guard = state;
        Ok(removed)
    }
    async fn submit_history_expiry_task(
        &self,
        request: HistoryExpiryTaskSubmission,
    ) -> Result<HistoryExpiryTaskAdmission, StorageError> {
        expiry::submit(self, request).await
    }
    async fn recover_history_expiry_task(
        &self,
        batch: Uuid,
    ) -> Result<Option<StorageTask>, StorageError> {
        expiry::recover_task(self, batch).await
    }
    async fn process_history_expiry_task(
        &self,
        request: HistoryExpiryTaskRequest,
    ) -> Result<HistoryExpiryTaskCheckpoint, StorageError> {
        expiry::process_task(self, request).await
    }
    async fn admit_history_archive_execution(
        &self,
    ) -> Result<HistoryArchiveExecutionOwner, StorageError> {
        let scope = self.current_execution_scope();
        if scope.storage_callback_active() {
            return Err(StorageError::unavailable(
                "Archive admission cannot nest inside a storage callback",
            ));
        }
        let application = scope
            .application_access_override()
            .cloned()
            .ok_or_else(|| {
                StorageError::unavailable("Archive requires original application admission")
            })?;
        let mut state = self.state.write_history_admission().await?;
        state.ensure_application_permit(&application)?;
        let epoch = HistoryReadEpoch::for_publication(
            HistoryPublicationKind::ArchiveRead,
            application,
            &state.history_lifecycle_snapshot()?,
            state.history_publications.generation(),
        )
        .and_then(|epoch| epoch.bind_scope(&scope))
        .map_err(invalid_contract_value)?;
        let isolation = epoch
            .application()
            .owner()
            .and_then(|owner| state.runtime_registrations.get(&owner.id()))
            .and_then(|runtime| runtime.isolation.clone());
        let mut staged = state.history_publications.clone();
        let parent = staged.admit_isolated(
            HistoryPublicationKind::ArchiveRead,
            epoch.clone(),
            isolation,
        )?;
        let child = staged.admit_producer(&parent.context(), &epoch)?;
        let admitted = HistoryArchiveExecutionOwner::from_admitted(parent, child, scope)?;
        state.history_publications = staged;
        Ok(admitted)
    }
    async fn admit_history_publication(
        &self,
        kind: HistoryPublicationKind,
    ) -> Result<HistoryPublicationOwner, StorageError> {
        let scope = self.current_execution_scope();
        if scope.storage_callback_active() {
            return Err(StorageError::unavailable(
                "History admission cannot nest inside a storage callback",
            ));
        }
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
            .is_some_and(|runtime| {
                runtime.history_protocol.supports_online()
                    || kind == HistoryPublicationKind::ArchiveRead
            })
        {
            return Err(StorageError::unavailable(
                "Publication requires a history-aware registered runtime",
            ));
        }
        let epoch = HistoryReadEpoch::for_publication(
            kind,
            application,
            &state.history_lifecycle_snapshot()?,
            state.history_publications.generation(),
        )
        .and_then(|epoch| epoch.bind_scope(&scope))
        .map_err(invalid_contract_value)?;
        let isolation = epoch
            .application()
            .owner()
            .and_then(|owner| state.runtime_registrations.get(&owner.id()))
            .and_then(|runtime| runtime.isolation.clone());
        state
            .history_publications
            .admit_isolated(kind, epoch, isolation)
    }
    async fn settle_history_publication(
        &self,
        settlement: HistoryPublicationSettlement,
    ) -> Result<(), StorageError> {
        if self.current_execution_scope().storage_callback_active() {
            return Err(StorageError::unavailable(
                "History settlement must run outside storage callbacks",
            ));
        }
        let mut state = self.state.write_control().await;
        state
            .history_archive
            .require_no_publication_claim(settlement.context().id())?;
        state.history_publications.settle(settlement)
    }
    async fn admit_history_producer(
        &self,
        context: HistoryReadContext,
    ) -> Result<HistoryProducerOwner, StorageError> {
        let scope = self.current_execution_scope();
        if scope.storage_callback_active() {
            return Err(StorageError::unavailable(
                "History admission cannot nest inside a storage callback",
            ));
        }
        if scope.history_read_override() != Some(&context) {
            return Err(StorageError::unavailable(
                "Producer must inherit its original publication scope",
            ));
        }
        let mut state = self.state.write().await?;
        let epoch = HistoryReadEpoch::for_publication(
            context.kind(),
            context.epoch().application().clone(),
            &state.history_lifecycle_snapshot()?,
            state.history_publications.generation(),
        )
        .and_then(|epoch| epoch.bind_scope(&scope))
        .map_err(invalid_contract_value)?;
        state.history_publications.admit_producer(&context, &epoch)
    }
    async fn settle_history_producer(
        &self,
        settlement: &HistoryProducerSettlement,
    ) -> Result<(), StorageError> {
        if self.current_execution_scope().storage_callback_active() {
            return Err(StorageError::unavailable(
                "History settlement must run outside storage callbacks",
            ));
        }
        let mut state = self.state.write_control().await;
        state
            .history_archive
            .require_no_publication_claim(settlement.context().parent().id())?;
        state.history_publications.settle_producer(settlement)
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
    async fn admit_history_expiry(
        &self,
        request: HistoryExpiryAdmissionRequest,
    ) -> Result<HistoryExpiryAdmissionResult, StorageError> {
        expiry::admit(self, request).await
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
        change
            .validate_commit_at(current.observed_at())
            .map_err(|reason| StorageError::conflict(reason.as_str()))?;
        if change.next().authority().expiry() != change.expected().authority().expiry() {
            return Err(StorageError::conflict(
                "Generic policy control cannot change retained expiry",
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
        check: &mut impl FnMut(
            StorageIdentityHistoryKind,
            &StorageIdentityEvidence,
        ) -> Result<(), StorageError>,
        budget: &mut HistoryRecoveryExposureBudget,
        family: StorageIdentityHistoryKind,
        evidence: &StorageIdentityEvidence,
    ) -> Result<(), StorageError> {
        budget
            .observe_evidence(evidence)
            .map_err(|error| StorageError::unavailable(error.to_string()))?;
        check(family, evidence)
    }

    pub(super) fn require_history_exposure_safe(&self) -> Result<(), StorageError> {
        if self.history_publications.active() != 0 {
            return Err(StorageError::unavailable(
                "History publication drivers or producers lack positive retirement; keep quarantine",
            ));
        }
        self.require_history_current_copies_safe(self.history_lifecycle.authority().expiry())
    }

    fn require_history_current_copies_safe(
        &self,
        journal: &HistoryExpiryJournal,
    ) -> Result<(), StorageError> {
        if journal.intents().is_empty() {
            return Ok(());
        }
        let families = journal
            .intents()
            .iter()
            .flat_map(|intent| intent.manifest().members())
            .map(HistoryExpiryMember::family)
            .collect::<BTreeSet<_>>();
        self.require_history_copies_safe(&families, |family, evidence| {
            if journal.excludes_evidence(family, Some(evidence)) {
                return Err(StorageError::unavailable(
                    "Current or protected-open copy carries expired or unresolved history ancestry; keep quarantine for explicit reconciliation",
                ));
            }
            Ok(())
        })
    }

    /// Reuse the same bounded current/open inventory for admission, inspection
    /// and recovery. Incomplete dependency evidence always protects the copy.
    fn history_candidate_has_dependency(
        &self,
        family: StorageIdentityHistoryKind,
        evidence: &StorageIdentityEvidence,
    ) -> bool {
        // ponytail: up to 1000 candidates x 1000 inventory rows; index
        // dependency identities if measured contention warrants it.
        let Some(target) = evidence
            .subject()
            .and_then(|subject| subject.record_identity())
        else {
            return true;
        };
        self.require_history_copies_safe(&BTreeSet::from([family]), |family, copy| {
            let dependent = if family == StorageIdentityHistoryKind::ClassSchema {
                copy.references()
                    .get("/class_id")
                    .is_none_or(|class| Some(class) == evidence.references().get("/class_id"))
            } else {
                copy.subject()
                    .and_then(|subject| subject.record_identity())
                    .is_none_or(|current| {
                        current.resource() == target.resource()
                            && (current.version() == target.version()
                                || copy.restore_source_versions().contains(target.version())
                                || copy.has_unknown_restore_source()
                                || !self.history_identity_visible(family, Some(copy)))
                    })
            };
            if dependent {
                Err(StorageError::unavailable(
                    "History version has a retained dependency",
                ))
            } else {
                Ok(())
            }
        })
        .is_err()
    }

    fn require_history_copies_safe(
        &self,
        families: &BTreeSet<StorageIdentityHistoryKind>,
        mut check: impl FnMut(
            StorageIdentityHistoryKind,
            &StorageIdentityEvidence,
        ) -> Result<(), StorageError>,
    ) -> Result<(), StorageError> {
        let mut budget = HistoryRecoveryExposureBudget::default();
        use StorageIdentityHistoryKind as Family;
        for family in families {
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
                Self::check_history_exposure_copy(&mut check, &mut budget, *family, evidence)?;
            }
        }
        if families.iter().any(|family| {
            !matches!(
                family,
                Family::ClassSchema
                    | Family::ComputedDefinitions
                    | Family::ClassRelation
                    | Family::ObjectRelation
            )
        }) {
            for entry in &self.history {
                if families.contains(&entry.family())
                    && entry.valid_to.is_none()
                    && entry.operation != StorageHistoryOperation::Delete
                {
                    Self::check_history_exposure_copy(
                        &mut check,
                        &mut budget,
                        entry.family(),
                        &entry.logical_identity,
                    )?;
                } else {
                    budget
                        .observe(0)
                        .map_err(|error| StorageError::unavailable(error.to_string()))?;
                }
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
                    budget
                        .observe(0)
                        .map_err(|error| StorageError::unavailable(error.to_string()))?;
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
                check(family, &evidence)?;
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
                        &mut check,
                        &mut budget,
                        Family::ComputedDefinitions,
                        evidence,
                    )?;
                } else {
                    budget
                        .observe(0)
                        .map_err(|error| StorageError::unavailable(error.to_string()))?;
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
                    &mut check,
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
            let facts = gap.facts();
            let object_target = self.logical_identities.resource_target(
                StorageIdentityResourceKind::Object,
                facts.target().resource(),
            )?;
            let current = object_target.and_then(|target| match target {
                StorageIdentityTarget::Resource { id, .. } => self.objects.get(&id.id()),
                _ => None,
            });
            let Some(object) = current else {
                return Err(StorageError::unavailable(
                    "Original object UUID correspondence is missing",
                ));
            };
            let class_uuid = facts.attribution().class().ok_or_else(|| {
                StorageError::unavailable("Missing original class correspondence")
            })?;
            let collection_target = self.logical_identities.resource_target(
                StorageIdentityResourceKind::Collection,
                facts.attribution().collection(),
            )?;
            let class_target = self
                .logical_identities
                .resource_target(StorageIdentityResourceKind::Class, class_uuid)?;
            let collection = collection_target
                .and_then(|target| match target {
                    StorageIdentityTarget::Resource { id, .. } => self.collections.get(&id.id()),
                    _ => None,
                })
                .ok_or_else(|| {
                    StorageError::unavailable("Original collection correspondence is missing")
                })?;
            let class = class_target
                .and_then(|target| match target {
                    StorageIdentityTarget::Resource { id, .. } => self.classes.get(&id.id()),
                    _ => None,
                })
                .ok_or_else(|| {
                    StorageError::unavailable("Original class correspondence is missing")
                })?;
            let mut measured = |target| -> Result<_, StorageError> {
                let identity = self.logical_identities.record(target)?;
                let encoded = serde_json::to_vec(&identity)
                    .map_err(|_| {
                        StorageError::backend_failure("Cannot measure population correspondence")
                    })?
                    .len();
                bytes += encoded;
                if encoded > 65536 || bytes > HistoryObjectPopulationGap::MAX_FACT_BYTES {
                    return Err(StorageError::unavailable(
                        "Population correspondence exceeds its metadata bound",
                    ));
                }
                Ok(identity)
            };
            let proof = HistoryObjectAuthorizationRemapping::new(
                facts,
                context.epoch().clone(),
                HistoryAuthorizationResource::object(
                    object.id(),
                    object.collection_id(),
                    object.class_id(),
                    measured(object_target.unwrap())?,
                ),
                HistoryAuthorizationResource::collection(
                    collection.id(),
                    measured(collection_target.unwrap())?,
                ),
                HistoryAuthorizationResource::class(
                    class.id(),
                    class.collection_id(),
                    measured(class_target.unwrap())?,
                ),
            )
            .map_err(|_| {
                StorageError::unavailable("Population correspondence or provenance differs")
            })?;
            if matches!(query.collection_scope(), StorageHistoryCollectionScope::Visible(ids) if !ids.contains(&proof.collection_id()))
            {
                continue;
            }
            let gap = gap.with_remapping(proof).map_err(|_| {
                StorageError::unavailable(
                    "Population remapping differs from its original selection",
                )
            })?;
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
async fn install_expiry_read_fixture(
    storage: &MemoryStorage,
    change: HistoryLifecycleControlChange,
) {
    // Logical read/recovery fixture only; never productive native admission.
    let mut state = storage.state.write_control().await;
    state
        .append_retained_lifecycle_events(
            change.next().audit_events_since(change.expected()).unwrap(),
        )
        .unwrap();
    state.history_lifecycle = change.next().clone();
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn publication_claim_fixture() -> (
        MemoryStorage,
        HistoryArchiveExecutionOwner,
        HistoryArchiveCapture,
        HistoryArchivePublicationRequest,
    ) {
        use hubuum_storage_core::artifacts::{ArtifactIdentity, DestinationId};
        let storage = MemoryStorage::new();
        let collection = storage
            .create_collection(
                StorageCollectionCreate::new(
                    "original publication claim",
                    "fixed input",
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
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap();
        storage
            .update_collection(
                collection.id(),
                StorageCollectionUpdate::new(None, Some("later version".into())),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let owner = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(application),
                storage.admit_history_archive_execution(),
            )
            .await
            .unwrap();
        let permit = owner.permit();
        let capture = storage
            .run_in_scope(
                permit.execution_scope().clone(),
                storage.capture_history_archive(
                    HistoryArchiveCaptureRequest::new(
                        PrincipalIdentity::generate(),
                        HistoryArchiveSelection::new(
                            StorageBackupHistorySection::CollectionHistory,
                            identity.resource(),
                            HistoryArchivePoint::Exact {
                                version: identity.version(),
                            },
                        ),
                        HistoryArchiveCatalogGeneration::default(),
                        Utc::now() + chrono::Duration::hours(1),
                    ),
                    permit,
                ),
            )
            .await
            .unwrap();
        let manifest = HistoryArchivePublicationManifest::from_capture(
            &capture,
            permit,
            ArtifactIdentity::new(DestinationId::generate()),
            Utc::now(),
        )
        .unwrap();
        let intent = HistoryArchiveEncodedArtifactIntent::try_new(
            manifest.location().clone(),
            hubuum_storage_core::artifacts::ArtifactLimits::builder()
                .artifact_bytes(HISTORY_ARCHIVE_PUBLICATION_BYTES)
                .build()
                .unwrap(),
            b"neutral claim fixture only; not provider codec/ownership proof".to_vec(),
            manifest.request_commitment().unwrap(),
        )
        .unwrap();
        let request = HistoryArchivePublicationRequest::try_new(manifest, intent).unwrap();
        (storage, owner, capture, request)
    }

    #[tokio::test]
    async fn archive_publication_claim_commit_is_single_assignment_and_drop_remains_charged() {
        let (storage, owner, capture, manifest) = publication_claim_fixture().await;
        let permit = owner.permit().clone();
        let expected = manifest.clone();
        let claim = storage
            .run_in_scope(
                permit.execution_scope().clone(),
                storage.claim_history_archive_publication(manifest, &permit),
            )
            .await
            .unwrap();
        assert_eq!(claim.manifest(), expected.manifest());
        assert_eq!(claim.request(), &expected);
        assert_eq!(claim.permit().producer(), permit.producer());
        drop(claim); // A dropped claim proves neither no issuance nor completion.
        assert!(
            storage
                .run_in_scope(
                    permit.execution_scope().clone(),
                    storage.claim_history_archive_publication(expected.clone(), &permit)
                )
                .await
                .is_err()
        );
        let complete = owner
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        assert!(
            storage
                .settle_history_producer(complete.producer())
                .await
                .is_err()
        );
        assert!(
            storage
                .settle_history_publication(complete.into_publication())
                .await
                .is_err()
        );
        let state = storage.state.read_control().await;
        assert_eq!(state.history_publications.active(), 1);
        assert_eq!(state.history_publications.active_producers(), 1);
        assert_eq!(state.history_archive.claims.len(), 1);
        assert_eq!(
            state.history_archive.publication_claim_bytes,
            expected.retained_manifest_bytes().unwrap()
        );
        assert_eq!(
            state
                .history_archive
                .claims
                .values()
                .next()
                .unwrap()
                .manifest()
                .capture(),
            capture.id()
        );
    }

    #[rstest::rstest]
    #[case::absent_scope(false)]
    #[case::callback_scope(true)]
    #[tokio::test]
    async fn archive_publication_claim_rejection_does_not_reserve_or_pin(#[case] callback: bool) {
        let (storage, owner, _, manifest) = publication_claim_fixture().await;
        let permit = owner.permit();
        let scope = if callback {
            permit.execution_scope().clone().with_storage_callback()
        } else {
            StorageExecutionScope::default()
        };
        assert!(
            storage
                .run_in_scope(
                    scope,
                    storage.claim_history_archive_publication(manifest, permit)
                )
                .await
                .is_err()
        );
        let state = storage.state.read_control().await;
        assert!(state.history_archive.claims.is_empty());
        assert_eq!(state.history_archive.publication_claim_bytes, 0);
    }

    #[rstest::rstest]
    #[case::other_parent(false)]
    #[case::old_unbound_capture(true)]
    #[tokio::test]
    async fn archive_capture_rejects_other_actual_producer(
        #[case] unbound: bool,
        #[values(false, true)] reserve: bool,
    ) {
        let storage = MemoryStorage::new();
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(application);
        let owner = storage
            .run_in_scope(scope.clone(), storage.admit_history_archive_execution())
            .await
            .unwrap();
        let other = storage
            .run_in_scope(scope, storage.admit_history_archive_execution())
            .await
            .unwrap();
        assert_eq!(owner.permit().application(), other.permit().application());
        let principal = PrincipalIdentity::generate();
        let capture = storage
            .run_in_scope(
                owner.permit().execution_scope().clone(),
                storage.capture_history_archive(
                    HistoryArchiveCaptureRequest::new(
                        principal,
                        HistoryArchiveSelection::new(
                            StorageBackupHistorySection::CollectionHistory,
                            ResourceIdentity::generate(),
                            HistoryArchivePoint::AsOf { at: Utc::now() },
                        ),
                        HistoryArchiveCatalogGeneration::default(),
                        Utc::now() + chrono::Duration::hours(1),
                    ),
                    owner.permit(),
                ),
            )
            .await
            .unwrap();
        if unbound {
            storage
                .state
                .write_control()
                .await
                .history_archive
                .remove_execution_binding_for_fixture(capture.id());
        }
        let proof = if unbound {
            owner.permit()
        } else {
            other.permit()
        };
        let result = storage
            .run_in_scope(proof.execution_scope().clone(), async {
                if reserve {
                    storage
                        .reserve_history_archive_read(capture.id(), principal, 1, proof)
                        .await
                        .map(|_| ())
                } else {
                    storage
                        .history_archive_capture(capture.id(), principal, proof)
                        .await
                        .map(|_| ())
                }
            })
            .await;
        let bytes = storage
            .state
            .read_control()
            .await
            .history_archive
            .reserved_for_fixture(capture.id());
        let error = result.unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        assert_eq!(bytes, 0);
        for owner in [owner, other] {
            let completed = owner
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage
                .settle_history_producer(completed.producer())
                .await
                .unwrap();
            storage
                .settle_history_publication(completed.into_publication())
                .await
                .unwrap();
        }
        storage.unregister_runtime(registration).await.unwrap();
    }

    #[rstest::rstest]
    #[case::owned(0, true)]
    #[case::missing_scope(1, false)]
    #[case::callback(2, false)]
    #[case::different_parent(3, false)]
    #[case::retired(4, false)]
    #[tokio::test]
    async fn archive_five_entries_retain_exact_owner(
        #[case] mode: u8,
        #[case] allowed: bool,
        #[values(0u8, 1, 2, 3, 4)] operation: u8,
    ) {
        let storage = MemoryStorage::new();
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let original = StorageExecutionScope::default().with_application_access(application);
        let owner = storage
            .run_in_scope(original.clone(), storage.admit_history_archive_execution())
            .await
            .unwrap();
        let permit = owner.permit().clone();
        let principal = PrincipalIdentity::generate();
        let resource = ResourceIdentity::generate();
        let request = HistoryArchiveCaptureRequest::new(
            principal,
            HistoryArchiveSelection::new(
                StorageBackupHistorySection::CollectionHistory,
                resource,
                HistoryArchivePoint::AsOf { at: Utc::now() },
            ),
            HistoryArchiveCatalogGeneration::default(),
            Utc::now() + chrono::Duration::hours(1),
        );
        let capture = storage
            .run_in_scope(
                permit.execution_scope().clone(),
                storage.capture_history_archive(request.clone(), &permit),
            )
            .await
            .unwrap();
        assert_eq!(
            capture.coverage(Utc::now()),
            HistoryArchiveCoverageState::Unavailable
        );
        let other = if mode == 3 {
            Some(
                storage
                    .run_in_scope(original, storage.admit_history_archive_execution())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let scope = match mode {
            1 => StorageExecutionScope::default(),
            2 => permit.execution_scope().clone().with_storage_callback(),
            3 => other.as_ref().unwrap().permit().execution_scope().clone(),
            _ => permit.execution_scope().clone(),
        };
        let completed = owner
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        if mode == 4 {
            storage
                .settle_history_producer(completed.producer())
                .await
                .unwrap();
        }
        let outcome = storage
            .run_in_scope(scope, async {
                match operation {
                    0 => storage
                        .history_archive_catalog(resource, &permit)
                        .await
                        .map(|_| ()),
                    // Invalid shape is projected ONLY after productive entry validation.
                    1 => storage
                        .publish_history_archive(
                            HistoryArchiveCatalogGeneration::default(),
                            vec![],
                            &permit,
                        )
                        .await
                        .map(|_| ()),
                    2 => storage
                        .capture_history_archive(request, &permit)
                        .await
                        .map(|_| ()),
                    3 => storage
                        .history_archive_capture(capture.id(), principal, &permit)
                        .await
                        .map(|_| ()),
                    4 => storage
                        .reserve_history_archive_read(capture.id(), principal, 1, &permit)
                        .await
                        .map(|_| ()),
                    _ => unreachable!(),
                }
            })
            .await;
        let expected = if allowed && operation == 1 {
            Some(StorageErrorKind::InvalidInput)
        } else if allowed {
            None
        } else {
            Some(StorageErrorKind::Unavailable)
        };
        assert_eq!(outcome.err().map(|error| error.kind()), expected);
        let bytes = storage
            .state
            .read_control()
            .await
            .history_archive
            .reserved_for_fixture(capture.id());
        assert_eq!(bytes, u64::from(allowed && operation == 4));
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
        storage
            .settle_history_publication(completed.into_publication())
            .await
            .unwrap();
        if let Some(other) = other {
            let completed = other
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage
                .settle_history_producer(completed.producer())
                .await
                .unwrap();
            storage
                .settle_history_publication(completed.into_publication())
                .await
                .unwrap();
        }
        let counts = {
            let state = storage.state.read_control().await;
            (
                state.history_publications.active(),
                state.history_publications.active_producers(),
            )
        };
        storage.unregister_runtime(registration).await.unwrap();
        assert_eq!(counts, (0, 0));
    }

    #[rstest::rstest]
    #[case::missing_scope(0, false)]
    #[case::owned(1, true)]
    #[case::prior_callback(2, false)]
    #[case::existing_parent(3, false)]
    #[tokio::test]
    async fn archive_atomic_admission_commits_both_owners_or_neither(
        #[case] mode: u8,
        #[case] allowed: bool,
    ) {
        let storage = MemoryStorage::new();
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let original = StorageExecutionScope::default().with_application_access(application);
        let parent = if mode == 3 {
            Some(
                storage
                    .run_in_scope(
                        original.clone(),
                        storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
                    )
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let scope = match mode {
            0 => StorageExecutionScope::default(),
            2 => original.clone().with_storage_callback(),
            3 => original
                .clone()
                .with_history_read(parent.as_ref().unwrap().context()),
            _ => original,
        };
        let result = storage
            .run_in_scope(scope, storage.admit_history_archive_execution())
            .await;
        let succeeded = result.is_ok();
        let count = {
            let state = storage.state.read_control().await;
            (
                state.history_publications.active(),
                state.history_publications.active_producers(),
            )
        };
        if let Ok(owner) = result {
            owner
                .permit()
                .validate_scope(owner.permit().execution_scope())
                .unwrap();
            let drained = owner
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage
                .settle_history_producer(drained.producer())
                .await
                .unwrap();
            storage
                .settle_history_publication(drained.into_publication())
                .await
                .unwrap();
        }
        if let Some(parent) = parent {
            let drained = parent
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            storage.settle_history_publication(drained).await.unwrap();
        }
        let after = {
            let state = storage.state.read_control().await;
            (
                state.history_publications.active(),
                state.history_publications.active_producers(),
            )
        };
        storage.unregister_runtime(registration).await.unwrap();
        assert_eq!(succeeded, allowed);
        assert_eq!(
            count,
            (usize::from(allowed || mode == 3), usize::from(allowed))
        );
        assert_eq!(after, (0, 0));
    }

    #[tokio::test]
    async fn archive_atomic_prebuilt_child_cannot_clear_callback_scope() {
        let storage = MemoryStorage::new();
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        // Construct BEFORE the parent's scope exists; inheritance is poll-time.
        let prebuilt = storage.run_in_scope(StorageExecutionScope::default(), async {
            let marker = storage.current_execution_scope().storage_callback_active();
            (marker, storage.admit_history_archive_execution().await)
        });
        let (marker, result) = storage
            .run_in_scope(
                StorageExecutionScope::default()
                    .with_application_access(application)
                    .with_storage_callback(),
                prebuilt,
            )
            .await;
        let count = storage
            .state
            .read_control()
            .await
            .history_publications
            .active();
        storage.unregister_runtime(registration).await.unwrap();
        assert!(marker && result.is_err());
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn archive_producer_rejects_a_different_api_owner_and_cleans_up_after_settlement() {
        let storage = MemoryStorage::new();
        let user = storage
            .create_user(StorageUserCreate::new(
                None,
                format!("archive_scope_{}", Uuid::new_v4().simple()),
                "test-hash",
                None,
                None,
                hubuum_events_core::EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
            .into_parts();
        let principal = hubuum_domain::PrincipalId::new(user.id().id()).unwrap();
        let registration = storage.register_runtime().await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let first = storage
            .admit_api_request(principal, ApiRequestClass::History, 0)
            .await
            .unwrap();
        let second = storage
            .admit_api_request(principal, ApiRequestClass::History, 0)
            .await
            .unwrap();
        let scope = StorageExecutionScope::default()
            .with_application_access(application.clone())
            .with_api_request(first.clone());
        let publication = storage
            .run_in_scope(
                scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let parent = publication.context();
        let wrong = StorageExecutionScope::default()
            .with_application_access(application)
            .with_api_request(second.clone())
            .with_history_read(parent.clone());
        let refused = storage
            .run_in_scope(wrong, storage.admit_history_producer(parent.clone()))
            .await;
        let producer = storage
            .run_in_scope(
                scope.with_history_read(parent.clone()),
                storage.admit_history_producer(parent),
            )
            .await
            .unwrap();
        // Occupancy settlement is independent: completion must not reacquire it.
        storage.settle_api_request(first).await.unwrap();
        let child = producer
            .after_completed(std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_producer(&child).await.unwrap();
        let drained = publication
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_publication(drained).await.unwrap();
        storage.settle_api_request(second).await.unwrap();
        storage.unregister_runtime(registration).await.unwrap();
        storage
            .delete_user(StorageUserDelete::new(
                user.id(),
                hubuum_events_core::EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        assert!(refused.is_err());
    }

    #[tokio::test]
    async fn archive_producer_rejects_a_different_live_application_owner() {
        let storage = MemoryStorage::new();
        let original = storage.register_runtime().await.unwrap();
        let other = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(original.owner()))
            .await
            .unwrap();
        let replacement = storage
            .capture_application_execution(Some(other.owner()))
            .await
            .unwrap();
        let original_scope = StorageExecutionScope::default().with_application_access(permit);
        let publication = storage
            .run_in_scope(
                original_scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let context = publication.context();
        let mismatched = StorageExecutionScope::default()
            .with_application_access(replacement)
            .with_history_read(context.clone());
        let refused = storage
            .run_in_scope(mismatched, storage.admit_history_producer(context.clone()))
            .await;
        // A rejected mismatched scope must not consume the original slot.
        let producer = storage
            .run_in_scope(
                original_scope.with_history_read(context.clone()),
                storage.admit_history_producer(context),
            )
            .await
            .unwrap();
        let completed = producer
            .after_completed(std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_producer(&completed).await.unwrap();
        let drained = publication
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage.settle_history_publication(drained).await.unwrap();
        storage.unregister_runtime(original).await.unwrap();
        storage.unregister_runtime(other).await.unwrap();
        assert!(refused.is_err());
    }

    #[tokio::test]
    async fn legacy_archive_producer_slot_has_exact_single_assignment_and_settlement() {
        let storage = MemoryStorage::new();
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(permit.clone());
        let publication = storage
            .run_in_scope(
                scope.clone(),
                storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
            )
            .await
            .unwrap();
        let context = publication.context();
        let productive = scope.clone().with_history_read(context.clone());
        let producer = storage
            .run_in_scope(
                productive.clone(),
                storage.admit_history_producer(context.clone()),
            )
            .await
            .unwrap();
        assert!(
            storage
                .run_in_scope(
                    productive.clone(),
                    storage.admit_history_producer(context.clone())
                )
                .await
                .is_err()
        );
        assert_eq!(
            storage
                .state
                .read_control()
                .await
                .history_publications
                .active_producers(),
            1
        );
        let archive =
            HistoryArchiveExecutionOwner::from_admitted(publication, producer, scope).unwrap();
        let permit = archive.permit().clone();
        let catalog = storage
            .run_in_scope(
                permit.execution_scope().clone(),
                storage.history_archive_catalog(ResourceIdentity::generate(), &permit),
            )
            .await
            .unwrap();
        assert!(catalog.versions().is_empty());
        // No external producer/transport is issued by this ledger fixture.
        let completed = archive
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
        assert_eq!(
            storage
                .state
                .read_control()
                .await
                .history_publications
                .active_producers(),
            0
        );
        assert!(
            storage
                .run_in_scope(productive.clone(), storage.admit_history_producer(context))
                .await
                .is_err()
        );
        storage
            .settle_history_publication(completed.into_publication())
            .await
            .unwrap();
        assert!(
            storage
                .run_in_scope(
                    permit.execution_scope().clone(),
                    storage.history_archive_catalog(ResourceIdentity::generate(), &permit)
                )
                .await
                .is_err()
        );
        assert_eq!(
            storage
                .state
                .read_control()
                .await
                .history_publications
                .active(),
            0
        );
        storage.unregister_runtime(registration).await.unwrap();
    }

    #[tokio::test]
    async fn combined_archive_allows_online_registration_without_leaking_owner() {
        let storage = MemoryStorage::new();
        let legacy = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(legacy.owner()))
            .await
            .unwrap();
        let archive = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(permit),
                storage.admit_history_archive_execution(),
            )
            .await
            .unwrap();
        let permit = archive.permit().clone();
        let catalog = storage
            .run_in_scope(
                permit.execution_scope().clone(),
                storage.history_archive_catalog(ResourceIdentity::generate(), &permit),
            )
            .await
            .unwrap();
        let before = storage.read_application_control().await.unwrap();
        let attempt = storage.register_history_runtime(None).await;
        let rejection = match attempt {
            Ok(unexpected) => {
                storage.unregister_runtime(unexpected).await.unwrap();
                None
            }
            Err(error) => Some(error.kind()),
        };
        let count = storage
            .state
            .read()
            .await
            .unwrap()
            .runtime_registrations
            .len();
        let after = storage.read_application_control().await.unwrap();
        let completed = archive
            .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
            .await
            .unwrap();
        storage
            .settle_history_producer(completed.producer())
            .await
            .unwrap();
        storage
            .settle_history_publication(completed.into_publication())
            .await
            .unwrap();
        storage.unregister_runtime(legacy).await.unwrap();
        assert!(catalog.versions().is_empty());
        assert_eq!(rejection, None);
        assert_eq!(count, 1);
        assert_eq!(before, after);
    }

    #[rstest::rstest]
    #[case::existing_capture(true)]
    #[case::new_capture(false)]
    #[tokio::test]
    async fn retained_collection_expiry_cannot_be_read_through_archive(
        #[case] existing_capture: bool,
    ) {
        let storage = MemoryStorage::new();
        let principal = PrincipalIdentity::generate();
        let key = HistoryPolicyKey::new(
            StorageIdentityHistoryKind::Collection,
            HistoryPolicyScope::Deployment,
        )
        .unwrap();
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
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
        storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .apply(
                        HistoryPolicyApplyRequest::new(policy, None),
                        principal,
                        snapshot.fence(),
                        snapshot.observed_at(),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        let collection = storage
            .create_collection(
                StorageCollectionCreate::new(
                    "retained collection",
                    "original",
                    GroupId::new(1).unwrap(),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        storage
            .update_collection(
                collection.id(),
                StorageCollectionUpdate::new(None, Some("current".into())),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let entry = storage.state.read_control().await.history.iter()
            .find(|entry| matches!(&entry.value, MemoryHistoryValue::Collection(value) if value.id() == collection.id()) && entry.valid_to.is_some())
            .unwrap().clone();
        let identity = entry
            .logical_identity
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap();
        let registration = storage.register_history_runtime(None).await.unwrap();
        let application = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(application);
        let owner = storage
            .run_in_scope(scope.clone(), storage.admit_history_archive_execution())
            .await
            .unwrap();
        let permit = owner.permit().clone();
        let request = HistoryArchiveCaptureRequest::new(
            principal,
            HistoryArchiveSelection::new(
                StorageBackupHistorySection::CollectionHistory,
                identity.resource(),
                HistoryArchivePoint::Exact {
                    version: identity.version(),
                },
            ),
            HistoryArchiveCatalogGeneration::default(),
            Utc::now() + chrono::Duration::hours(1),
        );
        let capture = storage
            .run_in_scope(
                permit.execution_scope().clone(),
                storage.capture_history_archive(request.clone(), &permit),
            )
            .await
            .unwrap();
        assert_eq!(
            capture.coverage(Utc::now()),
            HistoryArchiveCoverageState::Complete
        );
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let until = entry.valid_to.unwrap();
        let candidate = HistoryExpiryCandidate::from_evidence(
            StorageIdentityHistoryKind::Collection,
            &entry.logical_identity,
            Some(HistoryRetentionAnchor::Closed(until)),
            false,
        )
        .unwrap()
        .unwrap()
        .with_read_span(HistoryExpiryReadSpan::closed(entry.valid_from, until).unwrap())
        .unwrap();
        let mut selection = HistoryExpirySelection::new(
            Uuid::new_v4(),
            HistoryExpiryOrigin::Manual { principal },
            HistoryLifecycleScan::new(key, 1).unwrap(),
            &snapshot,
        )
        .unwrap();
        let bytes = serde_json::to_vec(&entry.backup_history_row().unwrap().1)
            .unwrap()
            .len();
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
        install_expiry_read_fixture(
            &storage,
            snapshot
                .control()
                .admit_expiry(selection.finish().unwrap(), snapshot.fence())
                .unwrap(),
        )
        .await;
        if existing_capture {
            assert!(
                storage
                    .run_in_scope(
                        permit.execution_scope().clone(),
                        storage.history_archive_capture(capture.id(), principal, &permit)
                    )
                    .await
                    .is_err()
            );
        } else {
            let next = storage
                .run_in_scope(scope, storage.admit_history_archive_execution())
                .await
                .unwrap();
            let permit = next.permit();
            let captured = storage
                .run_in_scope(
                    permit.execution_scope().clone(),
                    storage.capture_history_archive(request, permit),
                )
                .await
                .unwrap();
            assert_eq!(
                captured.coverage(Utc::now()),
                HistoryArchiveCoverageState::Unavailable
            );
        }
    }

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
    #[rstest::rstest]
    #[case::review_expired(true)]
    #[case::hold_expired(false)]
    #[tokio::test]
    async fn prepared_policy_rechecks_temporal_protection_before_commit(
        #[case] review_expired: bool,
    ) {
        let storage = MemoryStorage::new();
        let principal = PrincipalIdentity::generate();
        let initial = storage.capture_history_lifecycle().await.unwrap();
        let prepared_at =
            initial.observed_at() - Duration::seconds(if review_expired { 301 } else { 10 });
        let key = proposal().key();
        if !review_expired {
            let hold = HistoryRetentionHold::new(
                Uuid::new_v4(),
                key,
                prepared_at - Duration::seconds(1),
                Some(prepared_at + Duration::seconds(1)),
                principal,
            )
            .unwrap();
            storage
                .commit_history_lifecycle(
                    initial
                        .control()
                        .create_hold(hold, initial.fence())
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        let snapshot = storage.capture_history_lifecycle().await.unwrap();
        let proposal = HistoryPolicyChange::new(
            key,
            snapshot
                .control()
                .authority()
                .policies()
                .current_revision(key),
            HistoryPolicyBinding::Indefinite,
            HistoryPolicyApplication::Retrospective {
                closed_through: prepared_at - Duration::seconds(1),
            },
        )
        .unwrap();
        let preview = HistoryPolicyPreview::issue(
            principal,
            proposal.clone(),
            snapshot.fence(),
            prepared_at,
            HistoryPreviewImpact::new(HistoryPreviewCoverage::Complete, 0, 0, vec![]).unwrap(),
        )
        .unwrap();
        let issued = storage
            .commit_history_lifecycle(
                snapshot
                    .control()
                    .issue_preview(preview.clone(), snapshot.fence())
                    .unwrap(),
            )
            .await
            .unwrap();
        let change = issued
            .control()
            .apply(
                HistoryPolicyApplyRequest::new(proposal, Some(preview.reference().clone())),
                principal,
                issued.fence(),
                prepared_at,
            )
            .unwrap();
        let error = storage.commit_history_lifecycle(change).await.unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Conflict);
        assert_eq!(
            storage.capture_history_lifecycle().await.unwrap().control(),
            issued.control()
        );
    }

    #[tokio::test]
    async fn inspection_accepts_trigger_created_deletion_points() {
        let storage = MemoryStorage::new();
        let collection = storage
            .create_collection(
                StorageCollectionCreate::new("point tombstone", "", GroupId::new(1).unwrap(), None),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        storage
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .unwrap()
            .into_value();
        let scan = storage
            .scan_history_lifecycle(
                HistoryLifecycleScan::new(
                    HistoryPolicyKey::new(
                        StorageIdentityHistoryKind::Collection,
                        HistoryPolicyScope::Deployment,
                    )
                    .unwrap(),
                    1000,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let tombstone = scan
            .candidates()
            .iter()
            .find(|candidate| {
                matches!(candidate.anchor(), Some(HistoryRetentionAnchor::Deleted(_)),)
            })
            .expect("native deletion event");
        let span = tombstone.read_span().unwrap();
        assert_eq!(span.valid_to(), Some(span.valid_from()));
    }

    #[tokio::test]
    async fn inspection_reports_unknown_restore_ancestry_as_unresolved() {
        let (storage, _, closed) =
            crate::deletion_control::tests::fixture_on(MemoryStorage::new(), true).await;
        {
            let mut state = storage.state.write_control().await;
            let row = state
                .history
                .iter_mut()
                .find(|row| row.id == closed.id)
                .unwrap();
            let mut raw = serde_json::to_value(&row.logical_identity).unwrap();
            let mut bytes = *VersionIdentity::generate().as_bytes();
            bytes[6] = (bytes[6] & 0x0f) | 0x80;
            raw["subject"]["version"] =
                serde_json::json!(VersionIdentity::from_bytes(bytes).unwrap());
            row.logical_identity = serde_json::from_value(raw).unwrap();
        }
        let scan = storage
            .scan_history_lifecycle(
                HistoryLifecycleScan::new(
                    HistoryPolicyKey::new(
                        StorageIdentityHistoryKind::Object,
                        HistoryPolicyScope::Deployment,
                    )
                    .unwrap(),
                    1000,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scan.unresolved_identities(), 1);
        assert!(!scan.complete());
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
                match HistoryExpiryCandidate::from_evidence(
                    family,
                    evidence,
                    anchor,
                    self.history_candidate_has_dependency(family, evidence),
                )
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
    pub(super) fn require_history_expiry_reapplied(&self) -> Result<(), StorageError> {
        let journal = self.history_lifecycle.authority().expiry();
        if journal.is_empty() {
            return Ok(());
        }
        let families = journal
            .intents()
            .iter()
            .flat_map(|intent| intent.manifest().members())
            .map(HistoryExpiryMember::family)
            .collect::<BTreeSet<_>>();
        if journal.intents().iter().any(|intent| !intent.is_complete()) {
            return Err(StorageError::unavailable(
                "History expiry lacks a verified first receipt; keep quarantine",
            ));
        }
        for intent in journal.intents() {
            for member in intent.manifest().members() {
                if expiry::has_exact_copy(self, member) {
                    return Err(StorageError::unavailable(
                        "Retained exact history copy has not been reapplied; keep quarantine",
                    ));
                }
            }
        }
        let excluded_row = |family, row: &StorageBackupRow| -> Result<bool, StorageError> {
            if !families.contains(&family) {
                return Ok(false);
            }
            let evidence = row
                .get("logical_identity")
                .filter(|raw| !raw.is_null())
                .cloned()
                .map(serde_json::from_value::<StorageIdentityEvidence>)
                .transpose()
                .map_err(|_| {
                    StorageError::unavailable(
                        "Restored history correspondence is unresolved; keep quarantine",
                    )
                })?;
            Ok(journal.excludes_evidence(family, evidence.as_ref()))
        };
        let ordinary = self
            .history
            .iter()
            .any(|row| journal.excludes_evidence(row.family(), Some(&row.logical_identity)));
        let computed = self.computed_history.iter().any(|row| {
            journal.excludes_evidence(
                StorageIdentityHistoryKind::ComputedDefinitions,
                row.logical_identity(),
            )
        });
        if ordinary || computed {
            return Err(StorageError::unavailable(
                "Restored history descendant remains outside exact cleanup; keep quarantine",
            ));
        }
        for row in &self.schema_history {
            if excluded_row(StorageIdentityHistoryKind::ClassSchema, row)? {
                return Err(StorageError::unavailable(
                    "Restored schema history remains restricted; keep quarantine",
                ));
            }
        }
        for (section, family) in [
            (
                StorageBackupHistorySection::ClassRelationHistory,
                StorageIdentityHistoryKind::ClassRelation,
            ),
            (
                StorageBackupHistorySection::ObjectRelationHistory,
                StorageIdentityHistoryKind::ObjectRelation,
            ),
        ] {
            for row in self.relation_history.get(&section).into_iter().flatten() {
                if excluded_row(family, row)? {
                    return Err(StorageError::unavailable(
                        "Restored relation history remains restricted; keep quarantine",
                    ));
                }
            }
        }
        Ok(())
    }

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
        install_expiry_read_fixture(&storage, change).await;
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
            install_expiry_read_fixture(&storage, change).await;
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
        let manifest = change.next().authority().expiry().intents()[0].manifest();
        let request = HistoryExpiryAdmissionRequest::new(
            Uuid::new_v4(),
            manifest.run(),
            manifest.origin().clone(),
            HistoryLifecycleScan::new(manifest.key(), 1000).unwrap(),
            change.fence(),
        )
        .unwrap();
        let application = storage
            .capture_application_execution(Some(aware.owner()))
            .await
            .unwrap();
        let scope = StorageExecutionScope::default().with_application_access(application);
        let legacy = storage.register_runtime().await.unwrap();
        let error = storage
            .run_in_scope(scope.clone(), storage.admit_history_expiry(request.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        storage.unregister_runtime(legacy).await.unwrap();
        assert!(
            storage
                .run_in_scope(scope, storage.admit_history_expiry(request))
                .await
                .unwrap()
                .manifest()
                .is_some()
        );
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
