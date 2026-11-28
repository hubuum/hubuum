use async_trait::async_trait;
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Jsonb, Nullable};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    StorageError, StorageTaskCapacitySnapshot, StorageTaskPolicy, StorageTaskPolicyUpdate,
    TaskAdmissionPolicy, TaskCapacityStorage,
};
use serde_json::Value;

use super::PostgresStorage;
use crate::PostgresStorageError;

#[derive(QueryableByName)]
struct PolicyRow {
    #[diesel(sql_type = Nullable<Jsonb>)]
    value: Option<Value>,
}

impl PolicyRow {
    fn into_policy(self) -> Result<StorageTaskPolicy, PostgresStorageError> {
        let value = self.value.ok_or_else(|| {
            PostgresStorageError::conflict("Task policy revision changed; refetch before updating")
        })?;
        serde_json::from_value(value)
            .map_err(|error| PostgresStorageError::invalid_persisted_value("task policy", error))
    }
}

#[async_trait]
impl TaskCapacityStorage for PostgresStorage {
    async fn initialize_task_policy(
        &self,
        policy: TaskAdmissionPolicy,
    ) -> Result<StorageTaskPolicy, StorageError> {
        let value = serde_json::to_value(policy)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        self.runtime()
            .with_transaction(async move |connection| {
                diesel::sql_query("SELECT public.hubuum_task_policy_initialize($1) AS value")
                    .bind::<Jsonb, _>(value)
                    .get_result::<PolicyRow>(connection)
                    .await?
                    .into_policy()
            })
            .await
            .map_err(StorageError::from)
    }

    async fn task_policy(&self) -> Result<StorageTaskPolicy, StorageError> {
        self.runtime()
            .with_connection(async |connection| {
                diesel::sql_query("SELECT public.hubuum_task_policy_read() AS value")
                    .get_result::<PolicyRow>(connection)
                    .await?
                    .into_policy()
            })
            .await
            .map_err(StorageError::from)
    }

    async fn task_capacity_snapshot(&self) -> Result<StorageTaskCapacitySnapshot, StorageError> {
        self.runtime()
            .with_connection(async |connection| {
                let row =
                    diesel::sql_query("SELECT public.hubuum_task_capacity_snapshot() AS value")
                        .get_result::<PolicyRow>(connection)
                        .await?;
                let value = row.value.ok_or_else(|| {
                    PostgresStorageError::internal("Task accounting is unavailable")
                })?;
                serde_json::from_value(value).map_err(|error| {
                    PostgresStorageError::invalid_persisted_value("task capacity snapshot", error)
                })
            })
            .await
            .map_err(StorageError::from)
    }

    async fn replace_task_policy(
        &self,
        update: StorageTaskPolicyUpdate,
    ) -> Result<StorageTaskPolicy, StorageError> {
        let value = serde_json::to_value(update.policy())
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let expected = update.expected_revision().get() as i64;
        self.runtime()
            .with_transaction(async move |connection| {
                diesel::sql_query("SELECT public.hubuum_task_policy_replace($1,$2) AS value")
                    .bind::<BigInt, _>(expected)
                    .bind::<Jsonb, _>(value)
                    .get_result::<PolicyRow>(connection)
                    .await?
                    .into_policy()
            })
            .await
            .map_err(StorageError::from)
    }
}
