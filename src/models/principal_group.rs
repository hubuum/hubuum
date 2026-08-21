use crate::models::ResourceRevision;
use crate::models::group::Group;
use crate::models::principal::Principal;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::services::storage_boundary::{
    group_from_storage, group_id_to_storage, principal_from_storage, principal_group_from_storage,
    principal_id_to_storage,
};
use crate::storage::{GroupStorage, PrincipalStorage, StorageContext, storage_handle};

use crate::traits::crud::SaveAdapter;
use serde::{Deserialize, Serialize};

/// A principal's membership in a group. Both human users and service accounts
/// participate through this single table.
#[derive(Serialize, Deserialize)]
pub struct PrincipalGroup {
    pub principal_id: i32,
    pub group_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

#[derive(Serialize, Deserialize)]
pub struct NewPrincipalGroup {
    pub principal_id: i32,
    pub group_id: i32,
}

impl SaveAdapter for NewPrincipalGroup {
    type Output = PrincipalGroup;

    async fn save_adapter_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .add_group_member(
                principal_id_to_storage(self.principal_id),
                group_id_to_storage(self.group_id),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(principal_group_from_storage)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .add_group_member(
                principal_id_to_storage(self.principal_id),
                group_id_to_storage(self.group_id),
                context,
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(principal_group_from_storage)
    }
}

impl PrincipalGroup {
    pub async fn principal<C>(&self, backend: &C) -> Result<Principal, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .get_principal(principal_id_to_storage(self.principal_id))
            .await
            .map_err(ApiError::from)
            .and_then(principal_from_storage)
    }

    pub async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .get_group(group_id_to_storage(self.group_id))
            .await
            .map_err(ApiError::from)
            .and_then(group_from_storage)
    }

    pub async fn save<C>(&self, backend: &C) -> Result<PrincipalGroup, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .add_group_member(
                principal_id_to_storage(self.principal_id),
                group_id_to_storage(self.group_id),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(principal_group_from_storage)
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .remove_group_member(
                principal_id_to_storage(self.principal_id),
                group_id_to_storage(self.group_id),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}
