use super::*;
use serde_json::json;

#[derive(Clone, Default)]
pub(super) struct MemoryHistoryArchive {
    catalogs: BTreeMap<ResourceIdentity, HistoryArchiveCatalog>,
    captures: BTreeMap<Uuid, HistoryArchiveCapture>,
    staging_bytes: usize,
    capture_reservations: BTreeMap<Uuid, usize>,
}

fn entry(state: &MemoryState, permit: &StorageExecutionPermit) -> Result<(), StorageError> {
    if permit.owner().is_none() {
        return Err(StorageError::unavailable(
            "Archive reads require a registered Legacy runtime",
        ));
    }
    state.ensure_application_permit(permit)
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
fn select(
    state: &MemoryState,
    selection: &HistoryArchiveSelection,
    examined: &mut usize,
) -> Result<HistoryArchiveInput, StorageError> {
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

#[async_trait]
impl HistoryArchiveStorage for MemoryStorage {
    async fn history_archive_catalog(
        &self,
        resource: ResourceIdentity,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        let state = self.state.read().await?;
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
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        let resource = descriptors
            .first()
            .ok_or_else(|| StorageError::invalid_input("Empty archive publication"))?
            .identity()
            .resource();
        let mut state = self.state.write().await?;
        entry(&state, permit)?;
        let current = state
            .history_archive
            .catalogs
            .get(&resource)
            .cloned()
            .unwrap_or_else(|| HistoryArchiveCatalog::empty(resource));
        let next = current.publish(expected, descriptors.clone())?;
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
        state
            .history_archive
            .catalogs
            .insert(resource, next.clone());
        Ok(next)
    }
    async fn capture_history_archive(
        &self,
        request: HistoryArchiveCaptureRequest,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        let mut state = self.state.write().await?;
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
        let capture = HistoryArchiveCapture::capture(request, &catalog, permit, input, now)?;
        let expired: Vec<_> = state
            .history_archive
            .captures
            .iter()
            .filter(|(_, c)| c.available_until() <= now)
            .take(16)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(old) = state.history_archive.captures.remove(&id) {
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
        permit: &StorageExecutionPermit,
    ) -> Result<Option<HistoryArchiveCapture>, StorageError> {
        let state = self.state.read().await?;
        entry(&state, permit)?;
        let Some(capture) = state
            .history_archive
            .captures
            .get(&id)
            .filter(|c| c.principal() == principal)
        else {
            return Ok(None);
        };
        capture.validate_access(principal, permit)?;
        Ok(Some(capture.clone()))
    }
    async fn reserve_history_archive_read(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        bytes: u64,
        permit: &StorageExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        let mut state = self.state.write().await?;
        entry(&state, permit)?;
        let capture = state
            .history_archive
            .captures
            .get_mut(&id)
            .filter(|c| c.principal() == principal)
            .ok_or_else(|| StorageError::not_found("Archive capture unavailable"))?;
        capture.validate_access(principal, permit)?;
        capture.reserve(bytes, Utc::now())?;
        Ok(capture.clone())
    }
}
