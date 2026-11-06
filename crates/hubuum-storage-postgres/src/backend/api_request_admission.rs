use async_trait::async_trait;
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Jsonb, Nullable, Text, Uuid as SqlUuid};
use diesel_async::RunQueryDsl;
use hubuum_domain::PrincipalId;
use hubuum_storage_core::{
    ApiRequestAdmissionStorage, ApiRequestClass, ApiRequestPolicy, StorageApiRequestPermit,
    StorageApiRequestPolicy, StorageApiRequestPolicyUpdate, StorageError, TaskPolicyRevision,
};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::PostgresStorage;
use crate::PostgresStorageError;

#[cfg(all(test, feature = "integration-test-support"))]
mod tests;

#[derive(QueryableByName)]
struct MaterialRow {
    #[diesel(sql_type = Nullable<Jsonb>)]
    material: Option<Value>,
}

fn policy(row: MaterialRow) -> Result<StorageApiRequestPolicy, PostgresStorageError> {
    let material = row.material.ok_or_else(|| {
        PostgresStorageError::conflict("API request policy changed; refetch before updating")
    })?;
    serde_json::from_value(material)
        .map_err(|error| PostgresStorageError::invalid_persisted_value("API request policy", error))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PermitWire {
    owner: Uuid,
    revision: TaskPolicyRevision,
    policy: ApiRequestPolicy,
}

#[async_trait]
impl ApiRequestAdmissionStorage for PostgresStorage {
    async fn api_request_policy(&self) -> Result<StorageApiRequestPolicy, StorageError> {
        self.runtime()
            .with_connection(async |connection| {
                policy(
                    diesel::sql_query("SELECT public.hubuum_api_request_policy_read() AS material")
                        .get_result::<MaterialRow>(connection)
                        .await?,
                )
            })
            .await
            .map_err(StorageError::from)
    }

    async fn replace_api_request_policy(
        &self,
        update: StorageApiRequestPolicyUpdate,
    ) -> Result<StorageApiRequestPolicy, StorageError> {
        let material = serde_json::to_value(update.policy())
            .map_err(|error| StorageError::internal(error.to_string()))?;
        self.runtime()
            .with_transaction(async move |connection| {
                policy(
                    diesel::sql_query(
                        "SELECT public.hubuum_api_request_policy_replace($1,$2) AS material",
                    )
                    .bind::<BigInt, _>(update.expected_revision().get() as i64)
                    .bind::<Jsonb, _>(material)
                    .get_result::<MaterialRow>(connection)
                    .await?,
                )
            })
            .await
            .map_err(StorageError::from)
    }

    async fn admit_api_request(
        &self,
        principal: PrincipalId,
        class: ApiRequestClass,
        input_bytes: u64,
    ) -> Result<StorageApiRequestPermit, StorageError> {
        let input_bytes = i64::try_from(input_bytes).map_err(|_| {
            StorageError::api_request(hubuum_storage_core::ApiRequestRejection::InputBytes)
        })?;
        self.runtime()
            .with_api_request_admission(async move |connection| {
                let row = diesel::sql_query(
                    "SELECT public.hubuum_api_request_admit($1,$2,$3) AS material",
                )
                .bind::<Integer, _>(principal.id())
                .bind::<Text, _>(class.as_str())
                .bind::<BigInt, _>(input_bytes)
                .get_result::<MaterialRow>(connection)
                .await?;
                let wire: PermitWire = serde_json::from_value(row.material.ok_or_else(|| {
                    PostgresStorageError::unavailable("API request admission is unavailable")
                })?)
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value("API request permit", error)
                })?;
                StorageApiRequestPermit::try_new(wire.owner, wire.revision, wire.policy).map_err(
                    |error| {
                        PostgresStorageError::invalid_persisted_value("API request permit", error)
                    },
                )
            })
            .await
            .map_err(StorageError::from)
    }

    async fn settle_api_request(
        &self,
        permit: StorageApiRequestPermit,
    ) -> Result<(), StorageError> {
        // Settlement remains available after the application's generation or
        // runtime has been fenced. It cannot perform application work.
        self.runtime()
            .with_control_transaction(async move |connection| {
                diesel::sql_query("SELECT public.hubuum_api_request_settle($1)")
                    .bind::<SqlUuid, _>(permit.owner())
                    .execute(connection)
                    .await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .map_err(StorageError::from)
    }
}
