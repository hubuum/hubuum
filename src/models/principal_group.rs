use crate::models::ResourceRevision;
use crate::models::group::Group;
use crate::models::principal::Principal;

use crate::errors::ApiError;
use crate::storage::{GroupStorage, StorageContext, storage_handle};

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
            .add_group_member(self.principal_id, self.group_id, None)
            .await
            .map_err(ApiError::from)
    }
}

impl PrincipalGroup {
    pub async fn principal<C>(&self, backend: &C) -> Result<Principal, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .group_member_principal(self.principal_id)
            .await
            .map_err(ApiError::from)
    }

    pub async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .load_group(self.group_id)
            .await
            .map_err(ApiError::from)
    }

    pub async fn save<C>(&self, backend: &C) -> Result<PrincipalGroup, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .add_group_member(self.principal_id, self.group_id, None)
            .await
            .map_err(ApiError::from)
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .remove_group_member(self.principal_id, self.group_id, None)
            .await
            .map_err(ApiError::from)
    }
}
