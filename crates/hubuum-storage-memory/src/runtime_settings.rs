use super::*;

/// Process-local runtime settings. Like the rest of this adapter, values are
/// shared only by handles over one in-memory store and are lost on restart.
#[derive(Clone)]
pub(super) struct MemoryRuntimeSettings {
    snapshot: StorageRuntimeSettingsSnapshot,
    changes: Vec<StorageRuntimeSettingsChange>,
}

impl MemoryRuntimeSettings {
    pub(super) fn new() -> Self {
        Self {
            snapshot: StorageRuntimeSettingsSnapshot::initial(Utc::now()),
            changes: Vec::new(),
        }
    }

    fn update(
        &mut self,
        request: &StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        let current = self.snapshot.record(request.group()).clone();
        if current.revision() != request.expected_revision() {
            return Err(StorageError::revision_conflict(
                format!(
                    "The {} runtime settings revision changed",
                    request.group().as_str()
                ),
                current.revision(),
            ));
        }
        if current.assignment() == request.assignment() {
            return Ok(StorageRuntimeSettingsUpdateOutcome::Unchanged(current));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let changed_at = Utc::now();
        let change = StorageRuntimeSettingsChange::try_new(
            revision,
            current.assignment().clone(),
            request.assignment().clone(),
            changed_at,
            StorageRuntimeSettingsActor::from_context(request.context()),
        )
        .map_err(|error| StorageError::internal(error.to_string()))?;
        let record = StorageRuntimeSettingsRecord::new(
            request.assignment().clone(),
            revision,
            changed_at,
            request.context().actor_user_id(),
        );
        let records = self
            .snapshot
            .records()
            .iter()
            .map(|existing| {
                if existing.group() == request.group() {
                    record.clone()
                } else {
                    existing.clone()
                }
            })
            .collect();
        self.snapshot = StorageRuntimeSettingsSnapshot::try_new(records)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        self.changes.push(change);
        Ok(StorageRuntimeSettingsUpdateOutcome::Committed(record))
    }

    fn changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Vec<StorageRuntimeSettingsChange> {
        self.changes
            .iter()
            .rev()
            .filter(|change| change.group() == query.group())
            .filter(|change| {
                query
                    .before()
                    .is_none_or(|before| change.revision() < before)
            })
            .take(query.limit())
            .cloned()
            .collect()
    }
}

#[async_trait]
impl RuntimeSettingsStorage for MemoryStorage {
    async fn load_runtime_settings(&self) -> Result<StorageRuntimeSettingsSnapshot, StorageError> {
        Ok(self.state.read().await?.runtime_settings.snapshot.clone())
    }

    async fn update_runtime_settings(
        &self,
        request: StorageRuntimeSettingsUpdate,
    ) -> Result<StorageRuntimeSettingsUpdateOutcome, StorageError> {
        self.state.write().await?.runtime_settings.update(&request)
    }

    async fn list_runtime_settings_changes(
        &self,
        query: StorageRuntimeSettingsHistoryQuery,
    ) -> Result<Vec<StorageRuntimeSettingsChange>, StorageError> {
        Ok(self.state.read().await?.runtime_settings.changes(query))
    }
}
