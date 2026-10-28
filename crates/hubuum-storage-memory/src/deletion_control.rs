use super::*;

#[async_trait]
impl DeletionControlStorage for MemoryStorage {
    async fn capture_deletion_control(&self) -> Result<DeletionControlCapture, StorageError> {
        let state = self.state.read().await;
        Ok(DeletionControlCapture::new(
            state.deletion_control.clone(),
            DeletionRecoveryPosition::memory(state.deletion_control.lineage())
                .map_err(invalid_contract_value)?,
        ))
    }

    async fn commit_deletion_control(
        &self,
        change: DeletionControlChange,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if &state.deletion_control != change.previous() {
            return Err(StorageError::conflict(
                "Deletion authority changed; recapture before retrying",
            ));
        }
        state.deletion_control = change.next().clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn competing_control_changes_cannot_both_commit() {
        let storage = MemoryStorage::new();
        let initial = storage.capture_deletion_control().await.unwrap();
        let subject = DeletionSubject::Version {
            identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
        };
        let first = initial
            .state()
            .record_intent(Uuid::new_v4(), subject.clone(), vec![Uuid::new_v4()])
            .unwrap();
        let second = initial
            .state()
            .record_intent(Uuid::new_v4(), subject, vec![Uuid::new_v4()])
            .unwrap();
        let (a, b) = tokio::join!(
            storage.commit_deletion_control(first),
            storage.commit_deletion_control(second)
        );
        assert_ne!(a.is_ok(), b.is_ok());
    }
}
