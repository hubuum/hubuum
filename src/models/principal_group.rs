use crate::models::ResourceRevision;
use crate::models::group::Group;
use crate::models::principal::Principal;
use hubuum_domain::{GroupId, PrincipalId};
use hubuum_storage_core::StoragePrincipalGroup;

use crate::errors::ApiError;
use crate::events::EventContext;

use crate::storage::{
    GroupMembershipStorage, GroupStorage, PrincipalStorage, StorageContext, storage_handle,
};

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

impl From<StoragePrincipalGroup> for PrincipalGroup {
    fn from(row: StoragePrincipalGroup) -> Self {
        Self {
            principal_id: row.principal_id().id(),
            group_id: row.group_id().id(),
            created_at: row.created_at().naive_utc(),
            updated_at: row.updated_at().naive_utc(),
            revision: row.revision(),
        }
    }
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
                PrincipalId::new(self.principal_id)?,
                GroupId::new(self.group_id)?,
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .map(PrincipalGroup::from)
    }

    async fn save_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        storage_handle(pool)
            .add_group_member(
                PrincipalId::new(self.principal_id)?,
                GroupId::new(self.group_id)?,
                context,
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .map(PrincipalGroup::from)
    }
}

impl PrincipalGroup {
    pub async fn principal<C>(&self, backend: &C) -> Result<Principal, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .get_principal(PrincipalId::new(self.principal_id)?)
            .await
            .map_err(ApiError::from)
            .map(Principal::from)
    }

    pub async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .get_group(GroupId::new(self.group_id)?)
            .await
            .map_err(ApiError::from)
            .map(Group::from)
    }

    pub async fn save<C>(&self, backend: &C) -> Result<PrincipalGroup, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .add_group_member(
                PrincipalId::new(self.principal_id)?,
                GroupId::new(self.group_id)?,
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .map(PrincipalGroup::from)
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .remove_group_member(
                PrincipalId::new(self.principal_id)?,
                GroupId::new(self.group_id)?,
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}
