use async_trait::async_trait;
use hubuum_storage_postgres::operations::task_execution as postgres_task_execution;

use crate::storage::{
    StorageError, StorageTask, StorageTaskClaim, StorageTaskCompletion, StorageTaskEventAppend,
    StorageTaskFailure, StorageTaskLease, StorageTaskLeaseDuration, StorageTaskStateUpdate,
    TaskExecutionStorage,
};

use super::PostgresStorage;

#[async_trait]
impl TaskExecutionStorage for PostgresStorage {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        postgres_task_execution::claim_next_task(self.runtime(), lease_duration)
            .await
            .map_err(StorageError::from)
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        postgres_task_execution::renew_task_lease(self.runtime(), lease, lease_duration)
            .await
            .map_err(StorageError::from)
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        postgres_task_execution::recover_expired_task_leases(self.runtime(), batch_size)
            .await
            .map_err(StorageError::from)
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        postgres_task_execution::append_task_event(self.runtime(), event)
            .await
            .map_err(StorageError::from)
    }

    async fn update_task_state(
        &self,
        update: StorageTaskStateUpdate,
    ) -> Result<StorageTask, StorageError> {
        postgres_task_execution::update_task_state(self.runtime(), update)
            .await
            .map_err(StorageError::from)
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        postgres_task_execution::complete_task(self.runtime(), completion)
            .await
            .map_err(StorageError::from)
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        postgres_task_execution::fail_task(self.runtime(), failure)
            .await
            .map_err(StorageError::from)
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        postgres_task_execution::purge_expired_export_outputs(self.runtime())
            .await
            .map_err(StorageError::from)
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        postgres_task_execution::purge_expired_backup_outputs(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::config::get_config;
    use crate::storage::postgres::{PostgresPoolSettings, init_postgres_pool_with_settings};

    #[tokio::test]
    async fn lease_pool_remains_available_when_an_execution_pool_is_exhausted() {
        let config = get_config().expect("test requires database configuration");
        let settings = PostgresPoolSettings::builder(config.database_url.clone())
            .max_size(1)
            .statement_timeout_ms(config.db_statement_timeout_ms)
            .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
            .build()
            .expect("single-connection execution pool settings should be valid");
        let execution_pool = init_postgres_pool_with_settings(&settings);
        let backend =
            PostgresStorage::with_operational_pool_settings(execution_pool.clone(), settings);
        let _execution_connection = execution_pool
            .get()
            .await
            .expect("execution connection should be available");

        timeout(
            Duration::from_secs(5),
            backend.runtime().task_lease_pool().get(),
        )
        .await
        .expect("lease checkout must not wait for the execution pool")
        .expect("lease pool should connect to the test database");
    }
}
