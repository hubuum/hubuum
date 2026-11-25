use super::{BTreeMap, ImportTaskResultId, StorageError, StorageImportTaskResult, TaskId};

/// Import receipts retain their original identities across backup and replay.
#[derive(Clone, Default)]
pub(crate) struct ImportResults {
    by_task: BTreeMap<i32, Vec<StorageImportTaskResult>>,
    last_id: i32,
}

impl ImportResults {
    pub(crate) fn for_task(&self, task: TaskId) -> &[StorageImportTaskResult] {
        self.by_task
            .get(&task.id())
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub(crate) fn rows(&self) -> impl Iterator<Item = &StorageImportTaskResult> {
        self.by_task.values().flatten()
    }

    /// A batch advances its own cursor until every result is ready to publish.
    pub(crate) fn id_cursor(&self) -> ImportResultIdCursor {
        ImportResultIdCursor(self.last_id)
    }

    pub(crate) fn insert(&mut self, result: StorageImportTaskResult) {
        self.last_id = self.last_id.max(result.id().id());
        self.by_task
            .entry(result.task_id().id())
            .or_default()
            .push(result);
    }
}

pub(crate) struct ImportResultIdCursor(i32);

impl ImportResultIdCursor {
    pub(crate) fn allocate(&mut self) -> Result<ImportTaskResultId, StorageError> {
        self.0 = self
            .0
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Import result identifiers exhausted"))?;
        ImportTaskResultId::new(self.0).map_err(|error| StorageError::internal(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ImportStorage, MemoryStorage, PrincipalId, StorageImportResult, StorageTaskCreateRequest,
        StorageTaskKind, TaskQueueStorage,
    };
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn a_batch_that_exhausts_ids_preserves_the_final_id_for_a_retry() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let storage = MemoryStorage::new();
                let task = storage
                    .create_task(
                        StorageTaskCreateRequest::builder(
                            StorageTaskKind::Import,
                            PrincipalId::new(1).unwrap(),
                            json!({}),
                            2,
                        )
                        .try_build(10)
                        .unwrap(),
                    )
                    .await
                    .unwrap()
                    .id();
                let restored = StorageImportTaskResult::builder(
                    ImportTaskResultId::new(i32::MAX - 1).unwrap(),
                    task,
                    "object",
                    "create",
                    "success",
                    Utc::now(),
                )
                .build();
                storage
                    .state
                    .write()
                    .await
                    .import_task_results
                    .insert(restored);
                let result =
                    StorageImportResult::builder(task, "object", "create", "success").build();
                assert!(
                    storage
                        .record_import_results(vec![result.clone(), result.clone()])
                        .await
                        .is_err()
                );
                storage.record_import_results(vec![result]).await.unwrap();
                let state = storage.state.read().await;
                let ids: Vec<_> = state
                    .import_task_results
                    .for_task(task)
                    .iter()
                    .map(|r| r.id().id())
                    .collect();
                assert_eq!(ids, [i32::MAX - 1, i32::MAX]);
            });
    }
}
