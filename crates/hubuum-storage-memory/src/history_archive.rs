use super::*;
use hubuum_storage_core::artifacts::ArtifactReference;
use serde_json::json;

#[derive(Clone)]
pub(super) struct RetainedPublication {
    request: HistoryArchivePublicationRequest,
    verified_manifest_digest: Option<[u8; 32]>,
}

impl RetainedPublication {
    pub(super) fn manifest(&self) -> &HistoryArchivePublicationManifest {
        self.request.manifest()
    }
}

#[derive(Clone, Default)]
pub(super) struct MemoryHistoryArchive {
    catalogs: BTreeMap<ResourceIdentity, HistoryArchiveCatalog>,
    pub(super) claims: BTreeMap<Uuid, RetainedPublication>,
    pub(super) publication_claim_bytes: usize,
    captures: BTreeMap<Uuid, HistoryArchiveCapture>,
    historical: BTreeMap<(ResourceIdentity, VersionIdentity), BTreeSet<ArtifactReference>>,
    staging_bytes: usize,
    capture_reservations: BTreeMap<Uuid, usize>,
}

impl MemoryHistoryArchive {
    pub(super) fn require_no_publication_claim(&self, parent: Uuid) -> Result<(), StorageError> {
        if self.claims.contains_key(&parent) {
            return Err(StorageError::unavailable(
                "Publication still owns an unfinished artifact claim",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
impl MemoryHistoryArchive {
    pub(super) fn remove_execution_binding_for_fixture(&mut self, id: Uuid) {
        let mut wire =
            serde_json::to_value(self.captures.get(&id).expect("owned fixture capture")).unwrap();
        wire.as_object_mut().unwrap().remove("execution");
        self.captures
            .insert(id, serde_json::from_value(wire).unwrap());
    }
    pub(super) fn reserved_for_fixture(&self, id: Uuid) -> u64 {
        self.captures
            .get(&id)
            .expect("fixture capture")
            .reserved_read_bytes()
    }
}

fn entry(state: &MemoryState, permit: &HistoryArchiveExecutionPermit) -> Result<(), StorageError> {
    let owner = permit.application().owner().ok_or_else(|| {
        StorageError::unavailable("Archive reads require their original registered runtime")
    })?;
    state.ensure_application_permit(permit.application())?;
    if state
        .runtime_registrations
        .get(&owner.id())
        .is_none_or(|registration| {
            registration.history_protocol != owner.history_protocol()
                || (!registration.history_protocol.supports_online()
                    && !state.history_lifecycle.authority().expiry().is_empty())
        })
    {
        return Err(StorageError::unavailable(
            "Archive runtime ownership or history protocol is unavailable",
        ));
    }
    Ok(())
}
fn row(entry: &MemoryHistoryEntry) -> Result<Option<HistoryArchiveRow>, StorageError> {
    let MemoryHistoryValue::Collection(collection) = &entry.value else {
        return Ok(None);
    };
    let identity = entry
        .logical_identity
        .subject()
        .and_then(StorageIdentitySubject::record_identity)
        .ok_or_else(|| StorageError::unavailable("Collection history identity is unresolved"))?;
    HistoryArchiveRow::new(identity, entry.valid_from, entry.valid_to, json!({
        "id":collection.id().id(), "name":collection.name(), "description":collection.description(),
        "created_at":collection.created_at(), "updated_at":collection.updated_at(),
        "parent_collection_id":collection.parent_collection_id().map(CollectionId::id),
        "revision":collection.revision().get(), "logical_identity":entry.logical_identity
    })).map(Some)
}
fn expiry_pending(state: &MemoryState, resource: ResourceIdentity) -> bool {
    state
        .history_lifecycle
        .authority()
        .expiry()
        .intents()
        .iter()
        .flat_map(|intent| intent.manifest().members())
        .any(|member| {
            member.family() == StorageIdentityHistoryKind::Collection
                && member.target().resource() == resource
        })
}

fn select(
    state: &MemoryState,
    selection: &HistoryArchiveSelection,
    examined: &mut usize,
) -> Result<HistoryArchiveInput, StorageError> {
    if expiry_pending(state, selection.resource()) {
        return Ok(HistoryArchiveInput::Unavailable);
    }
    if selection.family() != StorageBackupHistorySection::CollectionHistory {
        return Ok(HistoryArchiveInput::Unavailable);
    }
    let mut created_at = None;
    for collection in state.collections.values() {
        if *examined == HISTORY_ARCHIVE_MAX_VERSIONS {
            return Err(StorageError::input_too_large(
                "History scope candidate budget exceeded",
            ));
        }
        *examined += 1;
        let evidence = state
            .logical_identities
            .record(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: collection.id().into(),
            })?;
        if evidence
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .is_some_and(|id| id.resource() == selection.resource())
        {
            created_at = Some(collection.created_at());
            break;
        }
    }
    let Some(created_at) = created_at else {
        return Ok(HistoryArchiveInput::Unavailable);
    };
    let mut selected = None;
    for entry in &state.history {
        if *examined == HISTORY_ARCHIVE_MAX_VERSIONS {
            return Err(StorageError::input_too_large(
                "History capture candidate budget exceeded",
            ));
        }
        *examined += 1;
        let Some(identity) = entry
            .logical_identity
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .filter(|id| id.resource() == selection.resource())
        else {
            continue;
        };
        let matches = match selection.point() {
            HistoryArchivePoint::Exact { version } => identity.version() == *version,
            HistoryArchivePoint::AsOf { at } => {
                entry.valid_from <= *at && entry.valid_to.is_none_or(|end| *at < end)
            }
        };
        if !matches {
            continue;
        }
        let MemoryHistoryValue::Collection(collection) = &entry.value else {
            continue;
        };
        let facts = HistoryArchiveAuthorization::new(collection.id(), identity, collection.name())?;
        let input = if collection.name().len() + collection.description().len()
            > HISTORY_ARCHIVE_ROW_BYTES
        {
            HistoryArchiveInput::BudgetLimited(facts)
        } else {
            match row(entry) {
                Ok(Some(row)) => HistoryArchiveInput::Available(row),
                Ok(None) => HistoryArchiveInput::Unavailable,
                Err(error) if error.kind() == StorageErrorKind::InputTooLarge => {
                    HistoryArchiveInput::BudgetLimited(facts)
                }
                Err(error) => return Err(error),
            }
        };
        if selected.replace(input).is_some() {
            return Err(StorageError::backend_failure(
                "Ambiguous collection history intervals",
            ));
        }
    }
    Ok(selected.unwrap_or_else(|| {
        if matches!(selection.point(),HistoryArchivePoint::AsOf{at} if *at<created_at) {
            HistoryArchiveInput::CompleteEmpty
        } else {
            HistoryArchiveInput::Unavailable
        }
    }))
}

fn copy_history(
    state: &MemoryState,
    target: StorageRecordIdentity,
    fence: ApplicationFence,
) -> Result<HistoryArchiveCopyHistory, StorageError> {
    if state.application_control.fence() != fence {
        return Err(StorageError::conflict(
            "Archive pin observation fence changed",
        ));
    }
    let pins = state
        .history_archive
        .captures
        .values()
        .filter_map(HistoryArchiveCapture::archive)
        .filter(|descriptor| descriptor.identity() == target)
        .map(|descriptor| descriptor.location().clone());
    let reclaimed = state
        .history_archive
        .historical
        .get(&(target.resource(), target.version()))
        .into_iter()
        .flatten()
        .cloned();
    let empty = HistoryArchiveCatalog::empty(target.resource());
    let catalog = state
        .history_archive
        .catalogs
        .get(&target.resource())
        .unwrap_or(&empty);
    HistoryArchiveCopyHistory::from_sources(target, fence, catalog, pins, reclaimed)
}

fn cleanup_claim(
    state: &MemoryState,
    request: &HistoryArchiveCleanupRequest,
) -> Result<HistoryArchiveCleanupClaim, StorageError> {
    if state.application_control.fence() != request.fence()
        || state.application_control.released_revision().is_some()
        || !state.runtime_registrations.is_empty()
    {
        return Err(StorageError::unavailable(
            "Offline archive cleanup requires the same quarantined image",
        ));
    }
    let claim = state
        .history_archive
        .claims
        .get(&request.publication())
        .ok_or_else(|| StorageError::unavailable("Original archive claim is unavailable"))?;
    let manifest = claim.manifest();
    let capture = state
        .history_archive
        .captures
        .get(&manifest.capture())
        .filter(|_| {
            state
                .history_archive
                .claims
                .values()
                .filter(|other| other.manifest().capture() == manifest.capture())
                .count()
                == 1
        })
        .ok_or_else(|| StorageError::unavailable("Original archive capture pin is unavailable"))?;
    let ownership = state
        .history_publications
        .isolated_archive_ownership(request, manifest)?;
    HistoryArchiveCleanupClaim::from_retained(
        request,
        manifest.clone(),
        capture,
        claim.request.intent().encoded().to_vec(),
        ownership,
        claim.verified_manifest_digest,
        &copy_history(state, manifest.identity(), request.fence())?,
    )
}

#[async_trait]
impl HistoryArchiveStorage for MemoryStorage {
    async fn history_archive_copy_history(
        &self,
        target: StorageRecordIdentity,
        fence: ApplicationFence,
    ) -> Result<HistoryArchiveCopyHistory, StorageError> {
        let state = self.state.read_control().await;
        copy_history(&state, target, fence)
    }

    async fn load_history_archive_cleanup(
        &self,
        request: &HistoryArchiveCleanupRequest,
    ) -> Result<HistoryArchiveCleanupClaim, StorageError> {
        let state = self.state.read_control().await;
        cleanup_claim(&state, request)
    }

    async fn settle_history_archive_cleanup(
        &self,
        settlement: HistoryArchiveCleanupSettlement,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write_control().await;
        let claim = settlement.claim();
        if cleanup_claim(&state, claim.request())? != *claim {
            return Err(StorageError::conflict(
                "Retained archive claim changed before cleanup settlement",
            ));
        }
        let id = claim.request().publication();
        let bytes = state.history_archive.claims[&id]
            .request
            .retained_manifest_bytes()?;
        let remaining = state
            .history_archive
            .publication_claim_bytes
            .checked_sub(bytes)
            .ok_or_else(|| StorageError::unavailable("Archive reservation accounting differs"))?;
        state
            .history_publications
            .settle_isolated_archive(&settlement)?;
        state.history_archive.claims.remove(&id);
        state.history_archive.publication_claim_bytes = remaining;
        Ok(())
    }

    async fn checkpoint_verified_history_archive(
        &self,
        checkpoint: HistoryArchiveVerifiedCheckpoint<'_>,
    ) -> Result<(), StorageError> {
        let claim = checkpoint.claim();
        let permit = claim.permit();
        let mut state = self.state.write_history_archive(permit).await?;
        entry(&state, permit)?;
        let manifest = claim.manifest();
        let capture = state
            .history_archive
            .captures
            .get(&manifest.capture())
            .ok_or_else(|| StorageError::unavailable("Original archive capture is unavailable"))?;
        manifest.validate_capture(capture, permit, Utc::now())?;
        let current = state
            .history_archive
            .catalogs
            .get(&manifest.identity().resource())
            .map(HistoryArchiveCatalog::generation)
            .unwrap_or_default();
        if current != manifest.generation()
            || expiry_pending(&state, manifest.identity().resource())
        {
            return Err(StorageError::conflict(
                "Archive catalog or expiry changed before checkpoint",
            ));
        }
        let retained = state
            .history_archive
            .claims
            .get_mut(&permit.producer().parent().id())
            .filter(|retained| &retained.request == claim.request())
            .ok_or_else(|| {
                StorageError::unavailable("Original archive publication claim is unavailable")
            })?;
        if retained
            .verified_manifest_digest
            .is_some_and(|saved| saved != *checkpoint.manifest_digest())
        {
            return Err(StorageError::conflict(
                "Verified archive checkpoint differs",
            ));
        }
        retained.verified_manifest_digest = Some(*checkpoint.manifest_digest());
        Ok(())
    }

    async fn claim_history_archive_publication(
        &self,
        request: HistoryArchivePublicationRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchivePublicationClaim, StorageError> {
        let mut state = self.state.write_history_archive(permit).await?;
        entry(&state, permit)?;
        let parent = permit.producer().parent().id();
        if state.history_archive.claims.contains_key(&parent) {
            return Err(StorageError::conflict(
                "Publication claim is already assigned",
            ));
        }
        let capture = state
            .history_archive
            .captures
            .get(&request.manifest().capture())
            .ok_or_else(|| StorageError::unavailable("Original archive capture is unavailable"))?;
        request.validate_capture(capture, permit, Utc::now())?;
        if state
            .history_archive
            .claims
            .values()
            .any(|claim| claim.manifest().capture() == capture.id())
        {
            return Err(StorageError::conflict("Archive capture is already claimed"));
        }
        let retained = request.retained_manifest_bytes()?;
        let total = state
            .history_archive
            .publication_claim_bytes
            .checked_add(retained)
            .filter(|total| {
                *total
                    <= HistoryPublicationRegistry::MAX_OWNERS
                        * (HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES + 128)
            })
            .ok_or_else(|| {
                StorageError::unavailable("Publication claim metadata capacity exhausted")
            })?;
        // The outer archive write gate checked this SAME original epoch while
        // holding the state lock. Never recapture application/API authority.
        state
            .history_publications
            .claim_archive_producer(permit.producer(), permit.producer().parent().epoch())?;
        state.history_archive.publication_claim_bytes = total;
        state.history_archive.claims.insert(
            parent,
            RetainedPublication {
                request: request.clone(),
                verified_manifest_digest: None,
            },
        );
        drop(state);
        HistoryArchivePublicationClaim::from_committed(request, permit)
    }

    async fn history_archive_catalog(
        &self,
        resource: ResourceIdentity,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        let state = self.state.read_history_archive(permit).await?;
        entry(&state, permit)?;
        Ok(state
            .history_archive
            .catalogs
            .get(&resource)
            .cloned()
            .unwrap_or_else(|| HistoryArchiveCatalog::empty(resource)))
    }
    async fn publish_history_archive(
        &self,
        expected: HistoryArchiveCatalogGeneration,
        descriptors: Vec<HistoryArchiveDescriptor>,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        let mut state = self.state.write_history_archive(permit).await?;
        entry(&state, permit)?;
        let resource = descriptors
            .first()
            .ok_or_else(|| StorageError::invalid_input("Empty archive publication"))?
            .identity()
            .resource();
        let current = state
            .history_archive
            .catalogs
            .get(&resource)
            .cloned()
            .unwrap_or_else(|| HistoryArchiveCatalog::empty(resource));
        let publication = current.publication(expected, descriptors.clone())?;
        let mut examined = 0;
        let mut bytes =
            history_archive_encoded_bytes(&descriptors, HISTORY_ARCHIVE_BATCH_BYTES)?.len();
        for descriptor in &descriptors {
            let selection = HistoryArchiveSelection::new(
                StorageBackupHistorySection::CollectionHistory,
                resource,
                HistoryArchivePoint::Exact {
                    version: descriptor.identity().version(),
                },
            );
            let hot = match select(&state, &selection, &mut examined)? {
                HistoryArchiveInput::Available(row) => row,
                HistoryArchiveInput::BudgetLimited(_) => {
                    return Err(StorageError::input_too_large(
                        "Publication input exceeds row budget",
                    ));
                }
                _ => {
                    return Err(StorageError::conflict(
                        "Publication requires retained primary version",
                    ));
                }
            };
            bytes += history_archive_encoded_bytes(&hot, HISTORY_ARCHIVE_ROW_BYTES)?.len();
            if bytes > HISTORY_ARCHIVE_BATCH_BYTES {
                return Err(StorageError::input_too_large(
                    "Publication input exceeds batch budget",
                ));
            }
            if !descriptor.matches_row(&hot)? {
                return Err(StorageError::conflict(
                    "Catalog publication requires the exact retained primary version",
                ));
            }
        }
        for (version, location) in publication.displaced() {
            state
                .history_archive
                .historical
                .entry((resource, *version))
                .or_default()
                .insert(location.clone());
        }
        let next = publication.into_catalog();
        state
            .history_archive
            .catalogs
            .insert(resource, next.clone());
        Ok(next)
    }
    async fn capture_history_archive(
        &self,
        request: HistoryArchiveCaptureRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        let mut state = self.state.write_history_archive(permit).await?;
        entry(&state, permit)?;
        let resource = request.selection().resource();
        let catalog = state
            .history_archive
            .catalogs
            .get(&resource)
            .cloned()
            .unwrap_or_else(|| HistoryArchiveCatalog::empty(resource));
        let input = select(&state, request.selection(), &mut 0)?;
        let now = Utc::now();
        let capture =
            HistoryArchiveCapture::capture(request, &catalog, permit.application(), input, now)?
                .bind_execution(permit)?;
        let expired: Vec<_> = state
            .history_archive
            .captures
            .iter()
            .filter(|(id, c)| {
                c.available_until() <= now
                    && !state
                        .history_archive
                        .claims
                        .values()
                        .any(|claim| claim.manifest().capture() == **id)
            })
            .take(16)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(old) = state.history_archive.captures.remove(&id) {
                if let Some(descriptor) = old.archive() {
                    let identity = descriptor.identity();
                    state
                        .history_archive
                        .historical
                        .entry((identity.resource(), identity.version()))
                        .or_default()
                        .insert(descriptor.location().clone());
                }
                state.history_archive.staging_bytes -= state
                    .history_archive
                    .capture_reservations
                    .remove(&old.id())
                    .expect("capture reservation");
            }
        }
        let bytes = state.history_archive.staging_bytes;
        let capture_bytes =
            history_archive_encoded_bytes(&capture, HISTORY_ARCHIVE_CAPTURE_BYTES)?.len() + 64;
        if state.history_archive.captures.len() >= HISTORY_ARCHIVE_MAX_CAPTURES
            || bytes + capture_bytes > HISTORY_ARCHIVE_STAGING_BYTES
        {
            return Err(capture.staging_budget_error());
        }
        state.history_archive.staging_bytes += capture_bytes;
        state
            .history_archive
            .capture_reservations
            .insert(capture.id(), capture_bytes);
        state
            .history_archive
            .captures
            .insert(capture.id(), capture.clone());
        Ok(capture)
    }
    async fn history_archive_capture(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<Option<HistoryArchiveCapture>, StorageError> {
        let state = self.state.read_history_archive(permit).await?;
        entry(&state, permit)?;
        let Some(capture) = state
            .history_archive
            .captures
            .get(&id)
            .filter(|c| c.principal() == principal)
        else {
            return Ok(None);
        };
        capture.validate_access(principal, permit.application())?;
        capture.validate_execution(permit)?;
        Ok(Some(capture.clone()))
    }
    async fn reserve_history_archive_read(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        bytes: u64,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        let mut state = self.state.write_history_archive(permit).await?;
        entry(&state, permit)?;
        let capture = state
            .history_archive
            .captures
            .get_mut(&id)
            .filter(|c| c.principal() == principal)
            .ok_or_else(|| StorageError::not_found("Archive capture unavailable"))?;
        capture.validate_access(principal, permit.application())?;
        capture.validate_execution(permit)?;
        capture.reserve(bytes, Utc::now())?;
        Ok(capture.clone())
    }
}
